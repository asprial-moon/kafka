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


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * A container that holds the list {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 * ProducerInterceptor 容器，负责按顺序调用用户配置的拦截器链。
 * KafkaProducer#doSend 之前会调用 onSend，发送完成或失败后会调用 onAcknowledgement/onSendError。
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    // 插件化包装后的 ProducerInterceptor 列表，封装了指标、关闭等通用插件生命周期能力。
    private final List<Plugin<ProducerInterceptor<K, V>>> interceptorPlugins;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors, Metrics metrics) {
        this.interceptorPlugins = Plugin.wrapInstances(interceptors, metrics, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#onSend(ProducerRecord)} method. ProducerRecord
     * returned from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on in the
     * interceptor chain. The record returned from the last interceptor is returned from this method.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * If an interceptor in the middle of the chain, that normally modifies the record, throws an exception,
     * the next interceptor in the chain will be called with a record returned by the previous interceptor that did not
     * throw an exception.
     *
     * 客户端调用 KafkaProducer#send 后、key/value 序列化之前会执行该方法。
     * 拦截器按链式顺序执行，前一个拦截器返回的 ProducerRecord 会作为下一个拦截器的输入；
     * 拦截器异常会被捕获并忽略，不能中断 Producer 的主发送流程。
     *
     * @param record the record from client
     * @return producer record to send to topic/partition
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        // 拦截器链是串联执行的：前一个拦截器返回的 record 会作为下一个拦截器的输入。
        ProducerRecord<K, V> interceptRecord = record;
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 用户拦截器可以修改 topic、partition、key、value、headers 等发送内容。
                interceptRecord = interceptorPlugin.get().onSend(interceptRecord);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                // be careful not to throw exception from here
                // 拦截器异常不能影响 Producer 主发送流程；这里只记录日志并继续执行后续拦截器。
                if (record != null)
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}
     * method for each interceptor.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     *
     * 当消息收到 broker 确认，或发送失败进入统一确认路径时，会调用该方法通知每个拦截器。
     * exception 为 null 表示发送成功；非 null 表示发送失败。拦截器异常只记录日志，不向外传播。
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 If an error occurred, metadata will only contain valid topic and maybe partition.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     * @param headers The headers for the record that was sent
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception, Headers headers) {
        // Sender 收到 broker 响应或发送失败后，会按顺序通知每个拦截器。
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // exception 为 null 表示发送成功；非 null 表示该 record 发送失败。
                interceptorPlugin.get().onAcknowledgement(metadata, exception, headers);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                // ack 阶段的拦截器异常同样不能反向影响 Producer 状态或用户 callback。
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend
     * (ProducerRecord)} method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}
     * method for each interceptor
     *
     * 当消息在进入正常 Sender ack 路径前失败时调用该方法。
     * 该方法会把发送前失败转换成 ProducerInterceptor#onAcknowledgement 调用，
     * 从而保证拦截器在发送成功、broker 返回失败、本地发送前失败三类场景下都能收到回调。
     *
     * @param record The record from client
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred
     *        after partition gets assigned; the topic part of interceptTopicPartition is the same as in record.
     * @param exception The exception thrown during processing of this record.
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        // onSendError 用于发送流程在进入正常 Sender ack 路径之前就失败的场景。
        // 它会把失败转换成 ProducerInterceptor#onAcknowledgement(metadata, exception, headers) 调用，
        // 从而保证拦截器无论成功、broker 失败还是本地发送前失败，都能收到统一的 acknowledgement 回调。
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                // 优先使用原 record 的 headers；record 为 null 时使用空 headers，保证回调参数非空。
                Headers headers = record != null ? record.headers() : new RecordHeaders();
                if (headers instanceof RecordHeaders && !((RecordHeaders) headers).isReadOnly()) {
                    // make a copy of the headers to make sure we don't change the state of origin record's headers.
                    // original headers are still writable because client might want to mutate them before retrying.
                    // 如果原 headers 仍可写，复制一份并设为只读，避免失败回调改变用户原始 record 的 headers 状态。
                    RecordHeaders recordHeaders = (RecordHeaders) headers;
                    headers = new RecordHeaders(recordHeaders);
                    ((RecordHeaders) headers).setReadOnly();
                }
                if (record == null && interceptTopicPartition == null) {
                    // record 和分区信息都不可用时，只能用 null metadata 表示发送失败。
                    interceptorPlugin.get().onAcknowledgement(null, exception, headers);
                } else {
                    if (interceptTopicPartition == null) {
                        // 如果 doSend 尚未计算出真实分区，就从原 record 中提取 topic 和用户指定分区。
                        interceptTopicPartition = extractTopicPartition(record);
                    }
                    // 构造失败占位元数据；offset、timestamp 等 broker 返回字段不可用。
                    interceptorPlugin.get().onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, -1, -1), exception, headers);
                }
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                // 失败通知阶段也不能让拦截器异常覆盖原始发送异常。
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * Extract topic partition from the original producer record when the actual append partition is not available.
     * 当发送失败发生在 RecordAccumulator 回填真实分区之前时，从原始 ProducerRecord 中提取 TopicPartition。
     * 如果用户未显式指定分区，则使用 UNKNOWN_PARTITION 保留“分区未知”的语义。
     */
    public static <K, V> TopicPartition extractTopicPartition(ProducerRecord<K, V> record) {
        // 发送前失败时可能尚未经过 RecordAccumulator 分区回填。
        // 如果用户没有显式指定分区，用 UNKNOWN_PARTITION 保留“分区未知”的语义。
        return new TopicPartition(record.topic(), record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
    }

    /**
     * Closes every interceptor in a container.
     * 关闭容器中的每个 ProducerInterceptor 插件。
     * 单个拦截器关闭失败只记录日志，不影响其他拦截器继续关闭。
     */
    @Override
    public void close() {
        // Producer 关闭时逐个关闭拦截器插件；单个拦截器关闭失败不影响其他拦截器释放。
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptorPlugin.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
