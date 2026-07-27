/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    private final long metadataIdleMs;

    /* Topics with expiry time */
    private final Map<String, Long> topics = new ConcurrentHashMap<>();
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;
    private Map<String, Errors> errors = null;

    public ProducerMetadata(long refreshBackoffMs,
                            long refreshBackoffMaxMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    /**
     * Add a topic to the metadata working set and refresh its expiry time.
     * 将 topic 加入 ProducerMetadata 的关注集合，并刷新它的过期时间。
     * KafkaProducer#doSend 在等待 metadata 前调用该方法，确保后续 metadata 请求会包含目标 topic。
     */
    public void add(String topic, long nowMs) {
        // Producer 发送某个 topic 前，会先把 topic 加入 metadata 关注集合。
        // 后续 metadata 请求会围绕该集合构造，确保能拿到发送所需的分区信息。
        Objects.requireNonNull(topic, "topic cannot be null");
        long expiryTime = nowMs + metadataIdleMs;
        // replace() only writes if the topic is still present, so there's no window in which a topic
        // concurrently evicted by retainTopic() could get silently re-inserted without newTopics bookkeeping.
        // 如果 topic 已存在，只刷新过期时间，避免活跃发送 topic 被 idle 清理。
        if (topics.replace(topic, expiryTime) != null) {
            return;
        }
        synchronized (this) {
            // New (or concurrently-evicted) topic: topics.put() and newTopics.add() must happen atomically
            // with retainTopic(), hence the shared lock.
            // 新 topic 必须同时写入 topics 和 newTopics，保证下一次 metadata 请求能精准包含它。
            if (topics.put(topic, expiryTime) == null) {
                newTopics.add(topic);
                requestUpdateForNewTopics();
            }
        }
    }

    /**
     * Atomically add a batch of topics to the working set, refreshing the
     * expiry for those already present. If any of the topics was newly added,
     * a partial metadata refresh is requested and the current updateVersion is
     * returned so the caller can pass it to {@link #awaitUpdate(int, long)} to
     * wait for the next response. Returns an empty {@code OptionalInt} when no
     * topic was newly added (no refresh requested, nothing to wait for).
     * 原子地批量加入 topic，并刷新已存在 topic 的过期时间。
     * 如果有新 topic 加入，会请求一次局部 metadata 刷新，并返回调用方可用于 awaitUpdate 的版本号。
     */
    public synchronized OptionalInt add(Collection<String> topics, long nowMs) {
        boolean anyNew = false;
        for (String topic : topics) {
            if (this.topics.put(topic, nowMs + metadataIdleMs) == null) {
                newTopics.add(topic);
                anyNew = true;
            }
        }
        return anyNew ? OptionalInt.of(requestUpdateForNewTopics()) : OptionalInt.empty();
    }

    /**
     * Request a metadata update for the given topic.
     * 请求刷新指定 topic 的 metadata。
     * 新 topic 走新 topic 局部刷新，已知 topic 走普通 metadata 更新。
     */
    public synchronized int requestUpdateForTopic(String topic) {
        // 新 topic 走局部 metadata 更新，避免不必要地刷新全部 topic；
        // 老 topic 则请求普通 metadata 更新，等待版本推进即可。
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate(false);
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        Long expireMs = topics.get(topic);
        if (expireMs == null) {
            return false;
        }
        if (newTopics.contains(topic)) {
            return true;
        }
        if (expireMs > nowMs) {
            return true;
        }
        // Only remove if the expiry we read is still current: the lock-free refresh path in add() can
        // race with this check, and a plain remove(topic) would drop a concurrently-refreshed entry.
        if (topics.remove(topic, expireMs)) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            return false;
        }
        return true;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 等待 metadata 更新完成，直到当前 metadata 版本大于调用方已知版本。
     * KafkaProducer#waitOnMetadata 使用该方法阻塞等待 Sender 线程收到新的 MetadataResponse。
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        // KafkaProducer#waitOnMetadata 会传入请求更新前的版本号。
        // 这里等待 metadata 的 updateVersion 变大，表示至少收到过一次新的 metadata 响应。
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            // fatal 错误不需要等到超时，发现后立即抛出。
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        // Producer 已关闭时，等待 metadata 的发送请求不再有意义，直接失败。
        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     * Update producer metadata from a MetadataResponse.
     * 根据 Sender 收到的 MetadataResponse 更新 Producer 侧 metadata 缓存和 topic 错误信息。
     * 更新完成后会唤醒等待 awaitUpdate 的发送线程。
     */
    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        // Sender 收到 MetadataResponse 后会调用这里更新缓存和 topic 错误信息。
        super.update(requestVersion, response, isPartialUpdate, nowMs);
        errors = response.errors();

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        // 一旦响应中包含某个新 topic，不论成功或带错误，都说明它已经经历过一次 metadata 响应。
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }

        // 唤醒 awaitUpdate 中等待 metadata 版本推进的发送线程。
        notifyAll();
    }

    public Errors getError(final String topic) {
        if (errors != null) {
            return errors.get(topic);
        }
        return null;
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
