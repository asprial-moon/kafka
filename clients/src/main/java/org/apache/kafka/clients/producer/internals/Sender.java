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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 * Producer 后台 I/O 线程：负责从 RecordAccumulator 拉取批次、构造 ProduceRequest、发送到 broker，并处理响应、重试和元数据刷新。
 */
public class Sender implements Runnable {

    private final Logger log;

    /* the client for sending requests to the Kafka cluster */
    /* 负责真实网络 I/O 的客户端封装，Sender 通过它连接 broker、发送请求并轮询响应。 */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    /* Producer 主线程 append 的消息批次缓冲区，Sender 从这里按 broker 维度 drain 可发送批次。 */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    /* Producer 侧的集群元数据缓存，提供 topic、partition、leader、topicId 等路由信息。 */
    private final ProducerMetadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    /* 是否需要保证同一分区请求顺序；开启时，已 drain 的分区会被 mute，避免后续批次越过前序批次。 */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    /* 单个 ProduceRequest 允许承载的最大请求大小，用于 drain 批次时控制请求体大小。 */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    /* acks 的典型取值：0=不等 broker 响应，1=leader 写入即确认，-1/all=等待 ISR 副本确认。 */
    /* broker 确认级别：决定 ProduceRequest 是否等待响应以及需要多少副本确认。 */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    /* 单个批次在可重试错误下的最大重试次数。 */
    private final int retries;

    /* the clock instance used for getting the time */
    /* 时间组件，用于计算 linger、超时、重试退避、指标耗时等。 */
    private final Time time;

    /* true while the sender thread is still running */
    /* Sender 主循环运行标记；close 时会置为 false，让线程退出正常发送循环。 */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    /* 强制关闭标记；置为 true 后，不再等待未发送或未确认批次完成，而是直接失败这些批次。 */
    private volatile boolean forceClose;

    /* metrics */
    /* Sender 相关指标集合，用于记录请求、重试、错误、批次大小、延迟等生产者指标。 */
    private final SenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    /* ProduceRequest 等待 broker 响应的请求超时时间。 */
    private final int requestTimeoutMs;

    /* The max time to wait before retrying a request which has failed */
    /* 请求失败后的重试退避时间，避免失败场景下紧密循环重试。 */
    private final long retryBackoffMs;

    /* all the state related to transactions, in particular the producer id, producer epoch, and sequence numbers */
    /* 事务和幂等状态管理器，维护 producerId、epoch、sequence、事务请求队列和事务分区状态。 */
    private final TransactionManager transactionManager;

    // A per-partition queue of batches ordered by creation time for tracking the in-flight batches
    // in-flight 表按分区保存已交给 NetworkClient、但尚未最终完成的 batch。
    // 同一分区内列表按创建/发送顺序排列，因此超时扫描遇到第一个未过期 batch 后可以停止。
    // 按分区记录已发送但尚未最终完成的批次，用于超时检查、顺序控制、失败清理和事务状态推进。
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        // 返回指定分区当前仍在网络层等待结果的 batch；没有记录时返回空列表，避免暴露 null。
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        // batch 已经完成、失败或重新入队后，需要从 in-flight 跟踪表移除，避免后续超时扫描重复处理。
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                // 分区下没有在途 batch 后移除 map 项，保持 in-flight 状态紧凑。
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        // 网络层已不再持有该 batch 时，既移除 in-flight 跟踪，也归还底层 ByteBuffer。
        maybeRemoveFromInflightBatches(batch);
        this.accumulator.completeAndDeallocateBatch(batch);
    }

    private void maybeRemoveAndDeallocateBatchLater(ProducerBatch batch) {
        // batch 逻辑上已完成，但网络层可能仍持有 buffer 引用；先标记完成，稍后再释放内存。
        maybeRemoveFromInflightBatches(batch);
        this.accumulator.completeBatch(batch);
    }

    /**
     *  Get the in-flight batches that has reached delivery timeout.
     *  获取已经超过 delivery.timeout.ms 的 in-flight 批次；这些批次可能已发出但还没有 broker 响应。
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    // delivery.timeout.ms 从 batch 创建时开始计时，覆盖排队、发送、重试和等待响应全过程。
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        // 已过期 batch 从 in-flight 列表摘除，后续会统一 failBatch 唤醒用户 Future/callback。
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            // 在进入响应处理前，in-flight 列表中不应出现已完成 batch；出现说明状态维护有 bug。
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        // 同一分区的 in-flight batch 按创建时间有序；当前未过期，后续更晚创建的 batch 也不会过期。
                        // 记录这个 batch 的到期时间，供 Sender 计算下一次 poll 最长等待时间。
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    // 清理空分区队列，避免后续扫描无意义 map 项。
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            // drain 出来的 batch 即将进入 NetworkClient，按 topic-partition 追加到在途列表尾部。
            List<ProducerBatch> inflightBatchList = inFlightBatches.computeIfAbsent(batch.topicPartition,
                k -> new ArrayList<>());
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        // drain 结果按 brokerId 分组；in-flight 跟踪按 topic-partition 分组，因此这里需要展开转换。
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    private boolean hasPendingTransactionalRequests() {
        // 只有事务仍在进行且事务管理器还有请求时，关闭阶段才需要继续 runOnce 推进事务控制流。
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    /**
     * The main run loop for the sender thread
     * Sender 线程主循环。注意：run() 只在线程启动时进入一次，后续通过 while 循环反复执行 runOnce()。
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        // 正常工作阶段：只要 producer 未关闭，就持续执行一轮发送逻辑。
        // 这不是 CPU 自旋；runOnce() 末尾会进入 client.poll(timeout)，在线程无事可做时阻塞等待网络事件或超时。
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the transaction manager, accumulator or waiting for acknowledgment,
        // wait until these are completed.
        // 优雅关闭阶段：停止接收新消息后，仍要继续发送 accumulator 中未 drain 的批次，
        // 并等待已经发出去的请求拿到响应，避免遗漏用户 Future/callback。
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // Abort the transaction if any commit or abort didn't go through the transaction manager's queue
        // 事务收尾阶段：如果关闭时事务仍未完成，需要补发 abort，避免事务悬挂在 broker 端。
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                try {
                    // It is possible for the transaction manager to throw errors when aborting. Catch these
                    // so as not to interfere with the rest of the shutdown logic.
                    transactionManager.beginAbort();
                } catch (Exception e) {
                    log.error("Error in kafka producer I/O thread while aborting transaction when during closing: ", e);
                    // Force close in case the transactionManager is in error states.
                    forceClose = true;
                }
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        if (forceClose) {
            // We need to fail all the incomplete transactional requests and batches and wake up the threads waiting on
            // the futures.
            // 强制关闭阶段：不再等待 broker 响应，直接失败未完成事务请求和批次，
            // 从而唤醒正在等待 Future.get() 或 callback 完成的业务线程。
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     * 执行一轮 Sender 工作：先处理事务控制请求，再处理普通 Produce 数据，最后 poll 网络 I/O。
     *
     */
    void runOnce() {
        if (transactionManager != null) {
            try {
                // 先修复或推进幂等序列状态，避免前序批次失败后后续批次继续以错误 sequence 发送。
                transactionManager.maybeResolveSequences();

                // 读取事务状态机最近一次错误；后续 fatal/abortable 分支会基于它决定是否失败批次或恢复状态。
                RuntimeException lastError = transactionManager.lastError();

                // do not continue sending if the transaction manager is in a failed state
                // 事务处于 fatal error 时不能继续发送数据，只能失败本地批次并轮询网络完成必要清理。
                if (transactionManager.hasFatalError()) {
                    // fatal error 一旦存在，所有未完成批次都需要用这个根因失败，避免用户继续等待。
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    // 即使停止普通发送，也要继续 poll，让已在网络层的请求完成回调和连接清理。
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                // 某些可中止错误如果属于授权类异常，可以先失败相关请求并把事务状态转回未初始化。
                if (transactionManager.hasAbortableError() && shouldHandleAuthorizationError(lastError)) {
                    return;
                }

                // Check whether we need a new producerId. If so, we will enqueue an InitProducerId
                // request which will be sent below
                // 幂等/事务 Producer 需要 producerId 和 epoch；必要时入队 InitProducerId 请求。
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                // 事务请求优先级高于普通数据请求，例如 InitProducerId、AddPartitionsToTxn、EndTxn 等。
                // 如果本轮已发送或等待事务请求，则直接返回，避免普通数据越过事务状态。
                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        // 记录本轮发送逻辑的当前时间，后续 ready、drain、timeout、poll 都使用同一时间基准。
        long currentTimeMs = time.milliseconds();
        // 从 accumulator 中找出可发送批次并发送 ProduceRequest，返回下一次网络 poll 最多可阻塞多久。
        long pollTimeout = sendProducerData(currentTimeMs);
        // Sender 不是一直自旋，而是在这里阻塞等待网络事件、metadata 刷新时机、请求超时或 pollTimeout 到期。
        // producer.send() 中的 sender.wakeup() 会打断这个等待，使 Sender 更快进入下一轮 runOnce() 检查新批次。
        client.poll(pollTimeout, currentTimeMs);
    }

    // We handle {@code TransactionalIdAuthorizationException} and {@code ClusterAuthorizationException} by first
    // failing the inflight requests, then transition the state to UNINITIALIZED so that the user doesn't need to
    // instantiate the producer again.
    // 事务授权类错误可以转成未初始化状态，让调用方修正授权后仍有机会复用 producer。
    private boolean shouldHandleAuthorizationError(RuntimeException exception) {
        if (exception instanceof TransactionalIdAuthorizationException ||
                        exception instanceof ClusterAuthorizationException) {
            // 授权失败会让等待中的事务请求全部以认证异常完成，避免事务请求 Future 卡住。
            transactionManager.failPendingRequests(new AuthenticationException(exception));
            // 授权失败后，已在本地累积或在途的普通批次也不能继续发送。
            maybeAbortBatches(exception);
            // 转回未初始化，后续如果授权恢复，可以重新走 InitProducerId 流程。
            transactionManager.transitionToUninitialized(exception);
            return true;
        }
        return false;
    }

    private void failExpiredBatches(List<ProducerBatch> expiredBatches, long now, boolean deallocateBuffer) {
        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetIdempotentProducerId to understand why
        // we need to reset the producer id here.
        // delivery.timeout.ms 到期后，无论批次是否已经发送，只要未完成就必须失败，保证用户 Future/callback 不会无限等待。
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
            // 构造对用户可见的超时错误信息，说明批次从创建到现在已经超过允许交付时间。
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation. "
                + "The request has not been sent, or no server response has been received yet.";
            // deallocateBuffer 决定是否立刻释放缓冲区；in-flight 批次可能还被网络层引用，不能总是立即释放。
            failBatch(expiredBatch, new TimeoutException(errorMessage), false, deallocateBuffer);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches are fully resolved.
                // 事务/幂等场景下，重试中的批次超时会让 sequence 状态不确定，需要阻止新批次继续 drain。
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
    }

    private long sendProducerData(long now) {
        // 获取当前元数据快照：后续 ready、drain、请求构造都基于同一份 topic/partition/leader 视图。
        MetadataSnapshot metadataSnapshot = metadata.fetchMetadataSnapshot();
        // get the list of partitions with data ready to send
        // 检查哪些分区的批次已满足发送条件：batch 满、linger 到期、内存压力、flush/close 等。
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadataSnapshot, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        // 有待发送数据但 leader 未知时，主动请求 metadata 更新；否则批次无法路由到目标 broker。
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            // 重新 add topic 是为了保持该 topic 在 metadata 关注集合中，确保后续刷新会包含它。
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            this.metadata.requestUpdate(false);
        }

        // remove any nodes we aren't ready to send to
        // readyNodes 只是“有数据可发的 broker”，这里还要确认连接状态、限流、重连退避等网络条件是否允许发送。
        Iterator<Node> iter = result.readyNodes.iterator();
        // 记录所有暂不可发送 broker 中，距离下一次可尝试发送最近还要等多久。
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            // 当前准备检查的目标 broker 节点。
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                // Update just the readyTimeMs of the latency stats, so that it moves forward
                // every time the batch is ready (then the difference between readyTimeMs and
                // drainTimeMs would represent how long data is waiting for the node).
                // broker 暂不可用时，不 drain 它的批次，并记录下一次可尝试连接/发送的等待时间。
                this.accumulator.updateNodeLatencyStats(node.id(), now, false);
                // 从本轮 ready 集合移除，避免 drain 出发往不可用 broker 的批次。
                iter.remove();
                // 取最短等待时间，作为后续 client.poll(timeout) 的候选上限。
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            } else {
                // Update both readyTimeMs and drainTimeMs, this would "reset" the node
                // latency.
                // broker 可用时，允许本轮 drain 发往该 broker 的批次。
                this.accumulator.updateNodeLatencyStats(node.id(), now, true);
            }
        }

        // create produce requests
        // 按 broker 聚合 drain 出来的 ProducerBatch，后续一个 broker 对应一个 ProduceRequest。
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadataSnapshot, result.readyNodes, this.maxRequestSize, now);
        // 记录已发出但未完成的批次，便于响应回来后完成、失败、重试或超时处理。
        addToInflightBatches(batches);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            // 保序模式下，已 drain 的分区暂时静音，直到当前批次完成后再允许后续批次发送。
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        accumulator.resetNextBatchExpiryTime();
        // 同时检查两类超时：已经发出去的 in-flight 批次，以及还停留在 accumulator 中未发送的批次。
        // reset 后重新扫描 in-flight 与 accumulator，计算下一批最早过期时间。
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        // 尚未发出去但已经超过 delivery.timeout.ms 的批次。
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);

        // 未发送批次失败后可以立即释放缓冲区。
        failExpiredBatches(expiredBatches, now, true);
        // in-flight 批次失败时可能需要延迟释放缓冲区，因为网络客户端可能仍持有引用。
        failExpiredBatches(expiredInflightBatches, now, false);

        // 记录本轮 drain 出来的批次指标，例如批次数、记录数、压缩率、请求大小等。
        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
        // time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
        // sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
        // that aren't ready to send since they would cause busy looping.
        // pollTimeout 决定本轮发送后，Sender 最多在网络 poll 中等多久：
        // 有可继续发送的数据时为 0，马上进入下一轮；否则等待到下一批次 ready、下一批次过期或 broker 可重试的最近时间点。
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        // delivery timeout 也会限制 poll 等待时间，避免批次已经过期但 Sender 仍长时间阻塞。
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        // 防止时间计算出现负数；负数在这里等价于马上返回。
        pollTimeout = Math.max(pollTimeout, 0);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            // 已有 broker 具备可发送数据时，不阻塞等待 I/O，立刻进入下一轮以尽快把更多批次发出去。
            pollTimeout = 0;
        }
        // 构造并提交 ProduceRequest；真正的 socket 写入和响应回调由后续 client.poll() 驱动。
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is enqueued
     * 如果本轮需要处理事务控制请求，则返回 true，表示普通数据发送要让路。
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        if (transactionManager.hasInFlightRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
            // 同一时间已有事务请求在途时，只 poll 等待响应，避免事务状态机并发推进。
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        if (transactionManager.hasAbortableError()) {
            // 可中止错误下，尚未 drain 的普通数据直接失败，避免继续进入事务发送链路。
            accumulator.abortUndrainedBatches(transactionManager.lastError());
        } else if (transactionManager.isAborting()) {
            // 用户或关闭流程正在 abort 事务时，未发送批次按事务中止失败。
            accumulator.abortUndrainedBatches(new TransactionAbortedException());
        }

        // 从事务管理器取下一个控制请求，例如查 coordinator、初始化 producerId、添加分区、提交或中止事务。
        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        // 没有待处理事务控制请求时，本轮可以继续走普通 Produce 数据发送。
        if (nextRequestHandler == null)
            return false;

        // 事务请求处理器负责构造请求，并在响应回来后推进事务状态机。
        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        // 本轮事务请求的目标 broker；可能是事务 coordinator，也可能是任意可用 broker。
        Node targetNode = null;
        try {
            // 需要 coordinator 的请求必须发往对应 coordinator；其他请求可发往负载最低的 broker。
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds()).node();
            if (targetNode != null) {
                // 找到目标 broker 后，还要等待连接 ready；连接未就绪则本轮不发送该事务请求。
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                    // 目标 broker 暂不可用，把事务请求重新放回队列，等待后续轮次重试。
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                log.trace("Coordinator not known for {}, will retry {} after finding coordinator.", coordinatorType, requestBuilder.apiKey());
                // coordinator 未知时，先发起查找 coordinator，再重试当前事务请求。
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                log.trace("No nodes available to send requests, will poll and retry when until a node is ready.");
                transactionManager.retry(nextRequestHandler);
                // 当前没有任何 broker 可用，短暂 poll 等待网络状态变化或 metadata 更新。
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            // 重试请求在再次发送前遵守对应退避时间，避免紧密重试压垮 broker 或本地 CPU。
            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            long currentTimeMs = time.milliseconds();
            // 事务控制请求也走 NetworkClient；响应完成后由 TxnRequestHandler 推进事务状态机。
            ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,
                true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {} with correlation ID {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            // 记录当前在途事务请求的 correlationId，响应回来时用于校验和推进正确的事务请求。
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            // 发送事务请求后立即 poll 一次，尽快处理连接、写入和可能已经返回的响应。
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            // 发送前连接失败时，当前事务请求不算完成，需要查找/等待目标节点后重试。
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            // 需要 coordinator 的事务请求先入队 FindCoordinator，找到 coordinator 后再重试原请求。
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            // 非 coordinator 请求没有固定目标节点，短暂退避并请求 metadata 更新，等待可用 broker 出现。
            time.sleep(retryBackoffMs);
            metadata.requestUpdate(false);
        }

        // 把原事务请求重新放回事务管理器队列，等待下一轮 Sender 再次尝试。
        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            // fatal error 下，本地未完成批次全部失败，用户 Future/callback 会收到对应异常。
            accumulator.abortBatches(exception);
            // in-flight 跟踪表也要清空，避免后续响应或超时处理重复操作已经失败的批次。
            inFlightBatches.clear();
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     * 开始优雅关闭 Sender：先关闭 accumulator 阻止新 append，再唤醒 Sender 让关闭流程尽快推进。
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        // 关闭 accumulator 必须早于 running=false，否则 Sender 退出主循环后仍可能有新消息 append，导致 callback 遗漏。
        this.accumulator.close();
        this.running = false;
        // 唤醒可能正在 client.poll(timeout) 中阻塞的 Sender 线程，使它及时观察到 running=false。
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        // 标记强制关闭后，run() 的关闭阶段不会再等待未发送或在途批次自然完成。
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        // 暴露 Sender 主循环是否仍处于运行状态。
        return running;
    }

    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        // 等待目标 broker 连接 ready；如果连接无法在 requestTimeoutMs 内建立，本轮事务请求会重试。
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to check ApiVersions
                // This allows us to bump transactional epochs even if the coordinator is temporarily unavailable at
                // the time when the abortable error is handled
                // 事务 coordinator ready 后，事务管理器可以继续检查 ApiVersions 或推进 epoch 相关恢复逻辑。
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }

    /**
     * Handle a produce response
     * 处理 Produce 响应：把 broker 返回结果映射回每个 ProducerBatch，并决定成功、失败、重试或刷新 metadata。
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, Map<Uuid, String> topicNames, long now) {
        // 请求头中包含 correlationId，用于把响应和之前发送的 ProduceRequest 对应起来。
        RequestHeader requestHeader = response.requestHeader();
        // 当前 ProduceRequest 的关联 id，日志、完成 batch 和排查请求响应配对时都会使用。
        int correlationId = requestHeader.correlationId();
        if (response.wasTimedOut()) {
            // 请求在客户端侧超时，所有随请求发出的 batch 都按 REQUEST_TIMED_OUT 完成失败路径。
            log.trace("Cancelled request with header {} due to the last request to node {} timed out",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.REQUEST_TIMED_OUT, String.format("Disconnected from node %s due to timeout", response.destination())),
                        correlationId, now, null);
        } else if (response.wasDisconnected()) {
            // 连接断开时，无法确认 broker 是否写入成功，交给 completeBatch 判断是否可重试或最终失败。
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),
                        correlationId, now, null);
        } else if (response.versionMismatch() != null) {
            // 客户端和 broker 协议版本不匹配，当前请求无法按预期解析或处理。
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now, null);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            // acks != 0 时 broker 会返回 ProduceResponse，需要逐 topic/partition 找回对应 batch。
            if (response.hasResponse()) {
                // Sender should exercise PartitionProduceResponse rather than ProduceResponse.PartitionResponse
                // https://issues.apache.org/jira/browse/KAFKA-10696
                // 将协议响应体转换成 ProduceResponse，后续逐 topic/partition 解析分区级结果。
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                // This will be set by completeBatch.
                // 部分错误响应会携带新的 leader 信息，先收集起来，等响应解析完后统一更新 metadata。
                Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
                produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                    // 将协议层分区响应转换为 Sender 内部统一使用的 PartitionResponse。
                    ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode()),
                            p.baseOffset(),
                            p.logAppendTimeMs(),
                            p.logStartOffset(),
                            p.recordErrors()
                                .stream()
                                .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                                .collect(Collectors.toList()),
                            p.errorMessage(),
                            p.currentLeader());

                    // Version 13 drops topic name, and supports topic id.
                    // We need to find batch based on topic id and partition index only as
                    // topic name in the response will be empty.
                    // For older versions, topic id is zero, and we will find the batch based on the topic name.
                    // 新协议优先用 topicId + partition 找 batch；旧协议使用 topic name + partition 找 batch。
                    TopicPartition tp = (!r.topicId().equals(Uuid.ZERO_UUID) && topicNames.containsKey(r.topicId())) ?
                            new TopicPartition(topicNames.get(r.topicId()), p.index()) :
                            new TopicPartition(r.name(), p.index());

                    // 通过 topic/partition 找回请求发送时记录的 ProducerBatch。
                    ProducerBatch batch = batches.get(tp);
                    if (batch == null) {
                        throw new IllegalStateException("Can't find batch created for topic id " + r.topicId() +
                                " topic name " + r.name() + " partition " + p.index() + " using " + topicNames);
                    }
                    // 单个分区响应交给 completeBatch 处理，它会根据 error 决定完成、重试、失败或刷新 metadata。
                    completeBatch(batch, partResp, correlationId, now, partitionsWithUpdatedLeaderInfo);
                }));

                if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                    // broker 返回了新的 leader 信息时，本地 metadata 可以局部修正，减少等待下一次完整 metadata 刷新的延迟。
                    // 响应里的 node endpoint 转为 Node 对象，供 metadata 更新 leader 映射。
                    List<Node> leaderNodes = produceResponse.data().nodeEndpoints().stream()
                        .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                        .filter(e -> !e.equals(Node.noNode()))
                        .collect(
                            Collectors.toList());
                    // 局部更新分区 leader，并返回实际发生更新的分区集合。
                    Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
                    if (log.isTraceEnabled()) {
                        updatedPartitions.forEach(
                            part -> log.debug("For {} leader was updated.", part)
                        );
                    }
                }

                // 记录该 broker ProduceRequest 的端到端请求延迟。
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                // acks = 0 不等待 broker 响应；请求写出后即按成功完成本地 Future/callback，但不代表 broker 一定持久化成功。
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, null);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * 根据单个 batch 的 ProduceResponse 决定最终处理：
     * 成功则完成 Future/callback；可重试错误则重新入队；不可重试或重试耗尽则失败。
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     * @param partitionsWithUpdatedLeaderInfo This will be populated with partitions that have updated leader info.
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now, Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo) {
        // 从网络在途状态恢复为非 in-flight，后续才能被完成、重试或失败清理。
        batch.setInflight(false);
        // 当前分区响应的错误码，是判断成功、重试、失败和 metadata 刷新的核心依据。
        Errors error = response.error;

        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            // 批次过大但包含多条记录时，可以拆分成更小批次重新入队；这不消耗 retry 次数。
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                formatErrMsg(response));
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            maybeRemoveAndDeallocateBatch(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                // 可重试错误且未超过次数/超时限制时，批次重新进入 accumulator，等待后续 Sender 轮次再次发送。
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    formatErrMsg(response));
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                // 幂等场景下重复 sequence 通常表示 broker 已处理过该批次；为了避免误报失败，这里按成功完成但 offset/timestamp 可能无效。
                completeBatch(batch, response);
            } else {
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                // 不可重试或重试耗尽时，失败 batch 并触发用户 Future/callback；是否调整 sequence 取决于是否还能安全判断。
                failBatch(batch, response, batch.attempts() < this.retries, true);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                // metadata 类错误说明本地 leader/partition 视图可能过期，需要触发刷新。
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {} Going " +
                            "to request metadata update now", batch.topicPartition, error.exception(response.errorMessage).toString());
                }
                if (error.exception() instanceof NotLeaderOrFollowerException || error.exception() instanceof FencedLeaderEpochException) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", batch.topicPartition, error, response.currentLeader);
                    if (partitionsWithUpdatedLeaderInfo != null
                        && (response.currentLeader.leaderId() != -1 && response.currentLeader.leaderEpoch() != -1)) {
                        // 新 leader 信息先暂存，等待当前响应全部解析完后再统一更新 metadata。
                        partitionsWithUpdatedLeaderInfo.put(batch.topicPartition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(response.currentLeader.leaderId()), Optional.of(response.currentLeader.leaderEpoch())));
                    }
                }
                // 请求 metadata 更新；实际发送 metadata 请求仍由后续 NetworkClient.poll() 驱动。
                metadata.requestUpdate(false);
            }
        } else {
            // 无错误：完成 batch，触发 Future/callback，并释放相关缓冲区。
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        // 保序模式下，当前批次完成后解除分区静音，允许该分区后续批次继续发送。
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * Format the error from a {@link ProduceResponse.PartitionResponse} in a user-friendly string
     * e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    private String formatErrMsg(ProduceResponse.PartitionResponse response) {
        // response.error 是协议错误码；errorMessage 是 broker 返回的可选明细，拼接后便于日志定位。
        String errorMessageSuffix = (response.errorMessage == null || response.errorMessage.isEmpty()) ?
                "" : String.format(". Error Message: %s", response.errorMessage);
        return String.format("%s%s", response.error, errorMessageSuffix);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        // 可重试失败不会触发用户 callback，而是把 batch 放回 accumulator 等待下一轮 Sender 发送。
        // 将可重试批次放回 accumulator，等待重试退避、metadata 或 broker 状态满足后再次发送。
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        // 成功完成入口：推进事务/幂等状态，完成 batch 内每条 record 的 Future/callback，并释放内存。
        if (transactionManager != null) {
            // 事务/幂等 producer 需要在 batch 成功后推进 sequence、in-flight 和事务状态。
            transactionManager.handleCompletedBatch(batch, response);
        }

        if (batch.complete(response.baseOffset, response.logAppendTime)) {
            // batch.complete 会完成每条 record 对应的 Future/callback；成功后释放 ByteBuffer。
            maybeRemoveAndDeallocateBatch(batch);
        } else {
            // Always safe to call deallocate because the batch keeps track of whether or not it was deallocated yet
            // 如果 batch 已经被其他路径完成，这里只确保缓冲区最终可释放。
            this.accumulator.deallocate(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           boolean adjustSequenceNumbers,
                           boolean deallocateBatch) {
        // 将 broker 分区级错误和可选 record 级错误转换成用户可见异常。
        // adjustSequenceNumbers 表示是否还能安全修正后续 batch 的 sequence；deallocateBatch 表示是否可立即释放 buffer。
        final RuntimeException topLevelException;
        // 把 broker 的分区级错误转换成用户 Future/callback 可感知的异常类型。
        if (response.error == Errors.TOPIC_AUTHORIZATION_FAILED)
            topLevelException = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
        else if (response.error == Errors.CLUSTER_AUTHORIZATION_FAILED)
            topLevelException = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
        else
            topLevelException = response.error.exception(response.errorMessage);

        if (response.recordErrors == null || response.recordErrors.isEmpty()) {
            // 没有 record 级错误时，batch 中每条 record 都使用同一个顶层异常失败。
            failBatch(batch, topLevelException, adjustSequenceNumbers, deallocateBatch);
        } else {
            // 部分响应会包含 record 级错误，需要映射到 batch 内对应 record 的异常。
            Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
            for (ProduceResponse.RecordError recordError : response.recordErrors) {
                // The API leaves us with some awkwardness interpreting the errors in the response.
                // We cannot differentiate between different error cases (such as INVALID_TIMESTAMP)
                // from the single error code at the partition level, so instead we use INVALID_RECORD
                // for all failed records and rely on the message to distinguish the cases.
                final String errorMessage;
                if (recordError.message != null) {
                    // record 级错误消息优先级最高，能定位到 batch 内具体失败记录。
                    errorMessage = recordError.message;
                } else if (response.errorMessage != null) {
                    // 没有 record 级消息时，使用分区级错误消息。
                    errorMessage = response.errorMessage;
                } else {
                    // 最后退回错误码默认消息，保证异常信息不为空。
                    errorMessage = response.error.message();
                }

                // If the batch contained only a single record error, then we can unambiguously
                // use the exception type corresponding to the partition-level error code.
                if (response.recordErrors.size() == 1) {
                    // 只有一条 record 错误时，可以安全使用分区错误码对应的异常类型。
                    recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
                } else {
                    // 多条 record 错误无法精确区分错误类型，统一映射为 InvalidRecordException。
                    recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
                }
            }

            Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
                // ProducerBatch 完成失败时会按 batchIndex 查询每条 record 对应的异常。
                RuntimeException exception = recordErrorMap.get(batchIndex);
                if (exception != null) {
                    return exception;
                } else {
                    // If the response contains record errors, then the records which failed validation
                    // will be present in the response. To avoid confusion for the remaining records, we
                    // return a generic exception.
                    return new KafkaException("Failed to append record because it was part of a batch " +
                        "which had one more more invalid records");
                }
            };

            failBatch(batch, topLevelException, recordExceptions, adjustSequenceNumbers, deallocateBatch);
        }
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        boolean adjustSequenceNumbers,
        boolean deallocateBatch
    ) {
        // 没有 record 级异常明细时，batch 内所有 record 共用同一个顶层异常。
        // 没有 record 级异常映射时，batch 内所有 record 共享同一个异常。
        failBatch(batch, topLevelException, batchIndex -> topLevelException, adjustSequenceNumbers, deallocateBatch);
    }

    private void failBatch(
        ProducerBatch batch,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions,
        boolean adjustSequenceNumbers,
        boolean deallocateBatch
    ) {
        // 最终失败入口：completeExceptionally 成功后，用户 Future.get 和 callback 都会收到异常。
        // 记录错误指标，按 topic 维度统计失败记录数。
        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.completeExceptionally(topLevelException, recordExceptions)) {
            // completeExceptionally 会失败 batch 中每条 record 的 Future/callback。
            if (transactionManager != null) {
                try {
                    // This call can throw an exception in the rare case that there's an invalid state transition
                    // attempted. Catch these so as not to interfere with the rest of the logic.
                    // 事务/幂等场景下，失败 batch 还要修正 sequence 或事务状态，避免后续批次顺序错误。
                    transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);
                } catch (Exception e) {
                    log.debug("Encountered error when transaction manager was handling a failed batch", e);
                }
            }
            if (deallocateBatch) {
                // 网络客户端已不再引用该批次缓冲区，可以立即释放。
                maybeRemoveAndDeallocateBatch(batch);
            } else {
                // Fix for KAFKA-19012
                // The pooled ByteBuffer associated with this batch might still be in use by the network client so we
                // cannot allow it to be reused yet. We skip deallocating it now. When the request in the network client 
                // completes with a response, either completeBatch() or failBatch() will be called with deallocateBatch=true.
                // The buffer associated with the batch will be deallocated then.
                // 请求还可能被 NetworkClient 持有时，只标记完成，延后到网络层真正结束后再释放缓冲区。
                maybeRemoveAndDeallocateBatchLater(batch);
            }
        } else {
            if (deallocateBatch) {
                // 如果 batch 已经被其他路径完成，这里只负责释放可释放的缓冲区。
                this.accumulator.deallocate(batch);
            }
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     * 判断 batch 是否允许重试：未超过 delivery timeout、未超过 retries、尚未完成，并且错误在当前事务/幂等语义下可重试。
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            (transactionManager == null ?
                    response.error.exception() instanceof RetriableException :
                    transactionManager.canRetry(response, batch));
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     * 将按 broker 聚合好的批次转换为 ProduceRequest 并逐个发送。
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        // collated 的 key 是 brokerId，value 是该 broker 作为 leader 的分区批次列表。
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     * 为一个目标 broker 构造 ProduceRequest：按 topic/partition 填充 records，并注册响应回调。
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        // 一个目标 broker 对应一个 ProduceRequest；空 batch 列表无需构造网络请求。
        if (batches.isEmpty())
            return;

        // 响应只携带 topic/partition 级结果；这里保存映射，回调中才能定位到原 ProducerBatch。
        // 用于响应回来时从 TopicPartition 找回原始 ProducerBatch。
        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());
        // 为本次请求中的 topic 获取 topicId；旧 broker 或未知 topicId 时会使用 ZERO_UUID。
        Map<String, Uuid> topicIds = topicIdsForBatches(batches);

        // ProduceRequest 协议结构是 topic -> partition -> records。
        // ProduceRequest 的 topic 集合，后续按 topic 聚合多个 partition 的 records。
        ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
        for (ProducerBatch batch : batches) {
            // ProduceRequest 的层级是 topic -> partition -> records，这里把 ProducerBatch 转换成协议结构。
            TopicPartition tp = batch.topicPartition;
            // ProducerBatch 内部已经封装为 MemoryRecords，可直接放入协议请求。
            MemoryRecords records = batch.records();
            // 当前 topic 的 topicId，用于支持按 topicId 路由和响应匹配的新协议版本。
            Uuid topicId = topicIds.get(tp.topic());
            // 同一个 topic 下可能有多个 partition，先查找已有 topic 节点，避免重复创建。
            ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic(), topicId);

            if (tpData == null) {
                // 第一次遇到该 topic 时创建 topic 级请求数据，并同时设置 topicId 和 topicName。
                tpData = new ProduceRequestData.TopicProduceData()
                        .setTopicId(topicId).setName(tp.topic());
                tpd.add(tpData);
            }

            // 把当前分区编号和 records 加入 topic 下的 partition 数据列表。
            tpData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(tp.partition())
                    .setRecords(records));
            // 保存反查关系：handleProduceResponse 会用 TopicPartition 找回这里的 batch。
            recordsByPartition.put(tp, batch);
            // 标记 batch 已进入网络发送链路，后续超时检查会把它当作 in-flight 批次处理。
            batch.setInflight(true);
        }

        String transactionalId = null;
        boolean useTransactionV1Version = false;
        if (transactionManager != null && transactionManager.isTransactional()) {
            // 事务 producer 的 ProduceRequest 必须携带 transactionalId，broker 才能把写入纳入事务。
            transactionalId = transactionManager.transactionalId();
            // Transaction V2 未启用时，ProduceRequest 需要按事务 V1 版本构造以兼容 broker 语义。
            useTransactionV1Version = !transactionManager.isTransactionV2Enabled();
        }

        // 构造真正发送给 broker 的 ProduceRequest，包括 acks、请求超时、事务 id 和批次数据。
        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(timeout)
                        .setTransactionalId(transactionalId)
                        .setTopicData(tpd),
                useTransactionV1Version
        );
        // Fetch topic names from metadata outside callback as topic ids may change during the callback
        // for example if topic was recreated.
        // 在 callback 外复制 topicId->topicName 快照，避免响应处理期间 metadata 变化导致找 batch 不稳定。
        Map<Uuid, String> topicNames = metadata.topicNames();

        // 响应完成时会回到 handleProduceResponse，继续完成、重试或失败对应 batch。
        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, topicNames, time.milliseconds());

        // NetworkClient 使用字符串形式的 nodeId 标识目标连接。
        String nodeId = Integer.toString(destination);
        // acks != 0 时需要等待响应；acks = 0 时请求发送完成即可在本地完成 batch。
        // newClientRequest 的 expectResponse 参数由 acks != 0 决定；acks=0 时不会等待 broker response。
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        // 提交给 NetworkClient；真正的 socket 写入由后续 client.poll() 驱动。
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    private Map<String, Uuid> topicIdsForBatches(List<ProducerBatch> batches) {
        // Uuid.ZERO_UUID 是 Kafka 协议中的“未知/未使用 topicId”占位值。
        // 从本地 metadata 快照中为每个 topic 取 topicId；缺失时使用 ZERO_UUID 保持对旧协议兼容。
        return batches.stream()
                .collect(Collectors.toMap(
                        b -> b.topicPartition.topic(),
                        b -> metadata.topicIds().getOrDefault(b.topicPartition.topic(), Uuid.ZERO_UUID),
                        (existing, replacement) -> replacement)
                );
    }

    /**
     * Wake up the selector associated with this send thread
     * 唤醒 Sender 关联的网络 selector。
     * 这里的“唤醒”不是重新执行 run()，也不是启动新线程；Sender 线程已经在 run() 的 while 循环中运行。
     * 它的作用是打断 Sender 可能正在 client.poll(timeout) 中进行的 NIO 阻塞等待，
     * 让线程提前返回并进入下一轮 runOnce()，从 accumulator 检查新 batch、metadata 更新或关闭信号。
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     * Sender 使用的一组指标传感器，负责把发送链路中的 batch、record、请求延迟、重试和错误转换为 producer metrics。
     */
    private static class SenderMetrics {
        // 全局重试记录数指标；按 record 数记录，而不是按 batch 数。
        public final Sensor retrySensor;
        // 全局发送失败记录数指标。
        public final Sensor errorSensor;
        // record 在 accumulator 中排队到被 drain 的等待时间。
        public final Sensor queueTimeSensor;
        // ProduceRequest 从发送到收到响应的请求耗时。
        public final Sensor requestTimeSensor;
        // 每个 ProduceRequest 携带的 record 数。
        public final Sensor recordsPerRequestSensor;
        // ProducerBatch 估算字节大小。
        public final Sensor batchSizeSensor;
        // batch 实际/估算压缩率。
        public final Sensor compressionRateSensor;
        // batch 内最大单条 record 大小，用于观察大消息。
        public final Sensor maxRecordSizeSensor;
        // batch 因 MESSAGE_TOO_LARGE 被拆分的次数/速率。
        public final Sensor batchSplitSensor;
        // 指标注册表，集中创建和查询全局/topic/node 维度的 metric。
        private final SenderMetricsRegistry metrics;
        // 记录指标时使用的时间源。
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            // batch-size：观察每个 batch 的发送体大小，帮助判断 batch.size 是否合适。
            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            // compression-rate：观察压缩效果，值越低通常表示压缩收益越明显。
            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            // queue-time：观察 batch 在客户端内存中等待多久才被 Sender drain。
            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            // request-time：观察 ProduceRequest 网络往返和 broker 处理耗时。
            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            // records-per-request：观察一次请求合并了多少 record，体现批处理效果。
            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            // record-retries：记录发生重试的 record 数和速率。
            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            // errors：记录最终失败的 record 数和速率。
            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            // record-size：记录 batch 中最大 record 大小的分布。
            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            // requestsInFlight 是当前 NetworkClient 中尚未完成的请求数。
            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            // metadataAge 表示距离最近一次成功 metadata 更新过去了多少秒。
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            // batch-split-rate：记录大批次拆分频率，通常和 max.request.size/batch.size/消息大小有关。
            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            // topic 维度指标按需懒注册：某 topic 第一次被发送时，一次性注册该 topic 的所有相关指标。
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                // topic 作为 metric tag，便于按主题维度查看吞吐、压缩、重试和错误。
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                // records 汇总同一个 broker 请求内的 record 数，用于 records-per-request 指标。
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    // 全局指标不区分 topic，用于观察 producer 整体批处理质量和排队/压缩状态。
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            // 先记录全局重试，再补充 topic 维度重试；topic sensor 可能尚未注册，因此需要判空。
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            // 先记录全局错误，再补充 topic 维度错误；count 表示失败 record 数。
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            // requestTimeSensor 是全局请求延迟；node-X.latency 是单 broker 维度延迟。
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            // MESSAGE_TOO_LARGE 后拆分 batch 时记录，用于发现批次过大或单条消息过大的问题。
            this.batchSplitSensor.record();
        }
    }

    public static class SenderThread extends KafkaThread {

        public SenderThread(final String name, Runnable runnable, boolean daemon) {
            super(name, runnable, daemon);
        }
    }
}
