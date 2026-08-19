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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.AbstractRecords;
import org.apache.kafka.common.record.internal.CompressionRatioEstimator;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.CopyOnWriteMap;
import org.apache.kafka.common.utils.internals.ExponentialBackoff;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.internals.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public class RecordAccumulator {

    private final LogContext logContext;
    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final Compression compression;
    private final int lingerMs;
    private final ExponentialBackoff retryBackoff;
    private final int deliveryTimeoutMs;
    private final long partitionAvailabilityTimeoutMs;  // latency threshold for marking partition temporary unavailable
    // adaptive partitioning 中判定分区暂时不可用的延迟阈值：broker 超过该时长无法 drain，分区器将避开该分区。
    private final boolean partitionerRackAware;
    private final String rack;
    private final boolean enableAdaptivePartitioning;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<String /*topic*/, TopicInfo> topicInfoMap = new CopyOnWriteMap<>();
    private final ConcurrentMap<Integer /*nodeId*/, NodeLatencyStats> nodeStats = new CopyOnWriteMap<>();
    private final IncompleteBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private final Map<String, Integer> nodesDrainIndex;
    private final TransactionManager transactionManager;
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE; // the earliest time (absolute) a batch will expire.

    /**
     * Create a new record accumulator
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param partitionerConfig Partitioner config
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             PartitionerConfig partitionerConfig,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.logContext = logContext;
        this.log = logContext.logger(RecordAccumulator.class);
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.enableAdaptivePartitioning = partitionerConfig.enableAdaptivePartitioning;
        this.partitionAvailabilityTimeoutMs = partitionerConfig.partitionAvailabilityTimeoutMs;
        this.partitionerRackAware = partitionerConfig.rackAware;
        this.rack = partitionerConfig.rack;
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        nodesDrainIndex = new HashMap<>();
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    /**
     * Create a new record accumulator with default partitioner config
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this(logContext,
            batchSize,
            compression,
            lingerMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            deliveryTimeoutMs,
            new PartitionerConfig(),
            metrics,
            metricGrpName,
            time,
            transactionManager,
            bufferPool);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        metrics.addMetric(
            metrics.metricName("waiting-threads", metricGrpName,
                "The number of user threads blocked waiting for buffer memory to enqueue their records"),
            (config, now) -> free.queued());

        metrics.addMetric(
            metrics.metricName("buffer-total-bytes", metricGrpName,
                "The maximum amount of buffer memory the client can use (whether or not it is currently used)."),
            (config, now) -> free.totalMemory());

        metrics.addMetric(
            metrics.metricName("buffer-available-bytes", metricGrpName,
                "The total amount of buffer memory that is not being used (either unallocated or in the free list)."),
            (config, now) -> free.availableMemory());
    }

    /**
     * Set the resolved partition on append callbacks.
     * 将 RecordAccumulator 最终确定的真实分区回填到 append 回调中。
     * KafkaProducer#doSend 后续会通过该分区进行事务登记、异常回调和结果元数据构造。
     */
    private void setPartition(AppendCallbacks callbacks, int partition) {
        // RecordAccumulator 是最终确定分区的位置之一。
        // 通过回调把真实分区回填给 KafkaProducer.AppendCallbacks，供 doSend 后续事务登记和异常回调用。
        if (callbacks != null)
            callbacks.setPartition(partition);
    }

    /**
     * Check if partition concurrently changed, or we need to complete previously disabled partition change.
     * 检查 sticky 分区是否被并发线程切换，或是否需要完成之前被延迟的分区切换。
     * 返回 true 表示调用方应重新选择分区并重试 append。
     *
     * @param topic The topic
     * @param topicInfo The topic info
     * @param partitionInfo The built-in partitioner's partition info
     * @param deque The partition queue
     * @param nowMs The current time, in milliseconds
     * @param cluster The cluster metadata
     * @return 'true' if partition changed and we need to get new partition info and retry,
     *         'false' otherwise
     */
    private boolean partitionChanged(String topic,
                                     TopicInfo topicInfo,
                                     BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                     Deque<ProducerBatch> deque, long nowMs,
                                     Cluster cluster) {
        // 多线程同时向同一 topic 追加无 key 消息时，sticky partition 可能被其他线程切换。
        // 如果发现切换，就返回 true 让 append 外层循环重新选择分区。
        if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
            log.trace("Partition {} for topic {} switched by a concurrent append, retrying",
                    partitionInfo.partition(), topic);
            return true;
        }

        // We might have disabled partition switch if the queue had incomplete batches.
        // Check if all batches are full now and switch .
        // 如果之前因为队列中还有未满批次而延迟切换分区，这里在批次都满后补上切换。
        if (allBatchesFull(deque)) {
            topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, 0, cluster, true);
            if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
                log.trace("Completed previously disabled switch for topic {} partition {}, retrying",
                        topic, partitionInfo.partition());
                return true;
            }
        }

        return false;
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     * 将一条序列化后的记录追加到 Producer 的内存累加器中，并返回追加结果。
     * 如果分区未知，该方法会通过内置 sticky/adaptive 分区逻辑选择真实分区；
     * 如果现有批次可用则复用，否则可能阻塞申请 buffer 并创建新批次。
     *
     * @param topic The topic to which this record is being sent
     *              目标 topic，RecordAccumulator 会按 topic 维护 TopicInfo 和分区 batch 队列。
     * @param partition The partition to which this record is being sent or RecordMetadata.UNKNOWN_PARTITION
     *                  if any partition could be used
     *                  目标分区；如果是 UNKNOWN_PARTITION，append 内部会通过内置分区器选择真实分区。
     * @param timestamp The timestamp of the record
     *                  记录时间戳，最终写入 RecordBatch。
     * @param key The key for the record
     *            已序列化后的 key 字节数组，可能为 null。
     * @param value The value for the record
     *              已序列化后的 value 字节数组，可能为 null。
     * @param headers the Headers for the record
     *                已转换为数组形式的消息 headers；为 null 时会替换为空 headers。
     * @param callbacks The callbacks to execute
     *                  当前记录的回调适配器，同时用于回填最终分区。
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     *                       BufferPool 内存不足时允许阻塞等待的最长时间，来自剩余 max.block.ms。
     * @param nowMs The current time, in milliseconds
     *              调用 append 时的当前时间，用于批次创建时间、等待时间和超时判断。
     * @param cluster The cluster metadata
     *                当前元数据快照，供内置分区器选择分区和判断 broker 可用性。
     */
    public RecordAppendResult append(String topic,
                                     int partition,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     AppendCallbacks callbacks,
                                     long maxTimeToBlock,
                                     long nowMs,
                                     Cluster cluster) throws InterruptedException {
        // 每个 topic 对应一个 TopicInfo，其中包含该 topic 的内置分区器和按分区组织的 batch 队列。
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(topic, k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize, partitionerRackAware, rack)));

        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        // 记录正在 append 的线程数，避免 abortIncompleteBatches 与追加过程并发时漏掉未完成批次。
        appendsInProgress.incrementAndGet();
        // 新批次需要的 buffer。只有当无法追加到现有批次时才分配；被 ProducerBatch 接管后置空。
        ByteBuffer buffer = null;
        // headers 允许调用方传 null，但底层 RecordBatch 需要非 null 数组。
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // Loop to retry in case we encounter partitioner's race conditions.
            // 外层循环用于处理 sticky partition 在并发追加时发生切换的竞态。
            while (true) {
                // If the message doesn't have any partition affinity, so we pick a partition based on the broker
                // availability and performance.  Note, that here we peek current partition before we hold the
                // deque lock, so we'll need to make sure that it's not changed while we were waiting for the
                // deque lock.
                // 如果 KafkaProducer#partition 返回 UNKNOWN_PARTITION，说明消息没有固定分区亲和性。
                // 这里通过内置 sticky/adaptive 分区器选择当前有效分区；如果用户已指定分区则直接使用。
                final BuiltInPartitioner.StickyPartitionInfo partitionInfo;
                final int effectivePartition;
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    // peek 只读取当前 sticky 分区，不立刻切换；后续 updatePartitionInfo 会根据写入字节数决定是否切换。
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster);
                    // append 真正使用的分区，后续 dq、callback、batch 都以它为准。
                    effectivePartition = partitionInfo.partition();
                } else {
                    // 用户或自定义 partitioner 已指定分区时，不需要 sticky partition 信息。
                    partitionInfo = null;
                    effectivePartition = partition;
                }

                // Now that we know the effective partition, let the caller know.
                // 分区一旦确定，立即回填给调用方回调对象。
                setPartition(callbacks, effectivePartition);

                // check if we have an in-progress batch
                // 获取该 topic-partition 的批次队列；队尾通常是当前可继续追加的批次。
                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(effectivePartition, k -> new ArrayDeque<>());
                synchronized (dq) {
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    // 加锁后再次确认 sticky 分区没有被其他线程切换；如果切换则重试整个 append 流程。
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // 优先尝试追加到已有队尾批次，能复用批次就避免分配新 buffer。
                    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    if (appendResult != null) {
                        // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                        // 如果队列里仍有未满批次，暂缓 sticky 分区切换，以提升批次聚合效果。
                        // enableSwitch=true 表示该分区当前批次都满了，内置分区器可以考虑切换 sticky 分区。
                        boolean enableSwitch = allBatchesFull(dq);
                        // 更新本次追加字节数，供 sticky/adaptive 分区器判断是否达到 stickyBatchSize 或需要避开慢 broker。
                        topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                        return appendResult;
                    }
                }

                if (buffer == null) {
                    // 现有批次没有空间时，按 batch.size 和单条消息估算上界中的较大值分配新 buffer。
                    // 单条消息可能大于 batch.size，因此必须取二者最大值。
                    int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers));
                    log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, topic, effectivePartition, maxTimeToBlock);
                    // This call may block if we exhausted buffer space.
                    // BufferPool 空间不足时这里可能阻塞，最长受 KafkaProducer#doSend 传入的剩余 max.block.ms 限制。
                    buffer = free.allocate(size, maxTimeToBlock);
                    // Update the current time in case the buffer allocation blocked above.
                    // NOTE: getting time may be expensive, so calling it under a lock
                    // should be avoided.
                    // allocate 可能阻塞较久，刷新 nowMs，避免新批次创建时间和后续超时判断使用过期时间。
                    nowMs = time.milliseconds();
                }

                synchronized (dq) {
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    // 分配 buffer 期间 sticky 分区仍可能被其他线程切换，因此入队前再校验一次。
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // 使用新 buffer 创建 ProducerBatch，并把当前 record 作为该批次第一条消息。
                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks, buffer, nowMs);
                    // Set buffer to null, so that deallocate doesn't return it back to free pool, since it's used in the batch.
                    // 新 batch 已持有该 buffer，避免 finally 中把仍在使用的 buffer 归还给 BufferPool。
                    if (appendResult.newBatchCreated)
                        buffer = null;
                    // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                    // 更新 sticky 分区已写入字节数，必要时切换到下一个分区。
                    boolean enableSwitch = allBatchesFull(dq);
                    // 新 batch 场景同样要更新分区器统计，否则 sticky 分区无法按累计字节数推进。
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                    return appendResult;
                }
            }
        } finally {
            // 如果 buffer 没有被新 ProducerBatch 接管，必须归还给 BufferPool。
            free.deallocate(buffer);
            // append 结束，递减并发追加计数。
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * Append a new batch to the queue
     * 创建新的 ProducerBatch 并追加到指定分区队列。
     * 调用前分区必须已经确定；该方法会把当前记录作为新批次的第一条记录。
     *
     * @param topic The topic
     *              目标 topic。
     * @param partition The partition (cannot be RecordMetadata.UNKNOWN_PARTITION)
     *                  已解析出的真实分区，不能是 UNKNOWN_PARTITION。
     * @param dq The queue
     *           当前 topic-partition 对应的 batch 队列，调用方必须已持有该队列锁。
     * @param timestamp The timestamp of the record
     *                  当前记录时间戳。
     * @param key The key for the record
     *            当前记录 key 字节数组。
     * @param value The value for the record
     *              当前记录 value 字节数组。
     * @param headers the Headers for the record
     *                当前记录 headers。
     * @param callbacks The callbacks to execute
     *                  当前记录完成时要执行的回调适配器。
     * @param buffer The buffer for the new batch
     *               已从 BufferPool 申请到、将由新 ProducerBatch 接管的 ByteBuffer。
     * @param nowMs The current time, in milliseconds
     *              新批次创建时间。
     */
    private RecordAppendResult appendNewBatch(String topic,
                                              int partition,
                                              Deque<ProducerBatch> dq,
                                              long timestamp,
                                              byte[] key,
                                              byte[] value,
                                              Header[] headers,
                                              AppendCallbacks callbacks,
                                              ByteBuffer buffer,
                                              long nowMs) {
        assert partition != RecordMetadata.UNKNOWN_PARTITION;

        // 在创建新批次前再尝试一次追加已有批次。
        // 这是为了处理当前线程等待 buffer 期间，其他线程可能已经创建了可用批次的情况。
        RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
        if (appendResult != null) {
            // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
            return appendResult;
        }

        // 用分配好的 ByteBuffer 构建 MemoryRecordsBuilder，再包装为 ProducerBatch。
        MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer);
        ProducerBatch batch = new ProducerBatch(new TopicPartition(topic, partition), recordsBuilder, nowMs);
        // 当前 record 作为新 batch 的第一条消息，FutureRecordMetadata 会返回给用户。
        FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                callbacks, nowMs));

        // 新批次追加到该分区队列尾部，等待 Sender 线程 drain。
        dq.addLast(batch);
        // incomplete 集合跟踪所有尚未完成的批次，用于关闭、abort、超时等流程。
        incomplete.add(batch);

        // newBatchCreated=true 告诉 KafkaProducer#doSend 可以唤醒 Sender。
        return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, batch.estimatedSizeInBytes());
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer) {
        // 把 BufferPool 分配的 ByteBuffer 包装成可追加 record 的 MemoryRecordsBuilder。
        // ProducerBatch 后续通过它写入 records、压缩、关闭并生成 MemoryRecords。
        return MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * Check if all batches in the queue are full.
     * 检查分区队列中的批次是否都已满。
     * 只有队尾批次可能未满，因此只需要检查队尾即可。
     */
    private boolean allBatchesFull(Deque<ProducerBatch> deque) {
        // Only the last batch may be incomplete, so we just check that.
        ProducerBatch last = deque.peekLast();
        return last == null || last.isFull();
    }

     /**
     *  Try to append to a ProducerBatch.
     *
     *  If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     *  resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     *  and memory records built) in one of the following cases (whichever comes first): right before send,
     *  if it is expired, or when the producer is closed.
     *
     * 尝试把记录追加到队尾已有 ProducerBatch。
     * 如果批次空间不足则返回 null，并关闭该批次的继续追加能力，让调用方创建新批次。
     *
     * @param timestamp 当前记录时间戳
     * @param key 当前记录 key 字节数组
     * @param value 当前记录 value 字节数组
     * @param headers 当前记录 headers
     * @param callback 当前记录完成时的 callback
     * @param deque 当前分区的 batch 队列，调用方应持有该队列锁
     * @param nowMs 当前时间，用于更新 batch 最后追加时间
     * @return 追加成功时返回结果；队列为空或队尾 batch 空间不足时返回 null
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        // Producer 已关闭时，不允许继续把消息追加到本地缓冲。
        if (closed)
            throw new KafkaException("Producer closed while send in progress");
        // 只尝试追加队尾批次，因为队头批次可能已经 ready 或正在等待发送。
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            // 追加前的估算大小，用于计算本次追加实际增加了多少字节。
            int initialBytes = last.estimatedSizeInBytes();
            // ProducerBatch#tryAppend 返回 null 表示该批次空间不足，需要创建新批次。
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null) {
                // 队尾批次已满，关闭 record append，释放压缩 buffer 等追加期资源。
                last.closeForRecordAppends();
            } else {
                // 追加后的大小差值会反馈给内置分区器，用于 sticky partition 切换判断。
                int appendedBytes = last.estimatedSizeInBytes() - initialBytes;
                // 成功追加到已有 batch，newBatchCreated=false。
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, appendedBytes);
            }
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * Get a list of batches which have been sitting in the accumulator too long and need to be expired.
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                // expire the batches in the order of sending
                synchronized (deque) {
                    while (!deque.isEmpty()) {
                        ProducerBatch batch = deque.getFirst();
                        if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                            deque.poll();
                            batch.abortRecordAppends();
                            expiredBatches.add(batch);
                        } else {
                            maybeUpdateNextBatchExpiryTime(batch);
                            break;
                        }
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches into the accumulator.
     * @return the number of split batches.
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression.type(),
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        int targetSplitBatchSize = this.batchSize;

        if (bigBatch.isSplitBatch()) {
            targetSplitBatchSize = Math.max(bigBatch.maxRecordSize, bigBatch.estimatedSizeInBytes() / 2);
        }
        Deque<ProducerBatch> dq = bigBatch.split(targetSplitBatchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    // multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    // subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    // the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    // producer id. We will not attempt to reorder messages if the producer id has changed, we will throw an
    // IllegalStateException instead.
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are re-enqueueing and have enabled idempotence, the re-enqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (!transactionManager.hasInflightBatches(batch.topicPartition))
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to retry
            // the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each time.
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * Add the leader to the ready nodes if the batch is ready
     * 单个分区的 ready 判定核心：若队头批次满足发送条件，就把该分区的 leader broker 加入 readyNodes。
     * 发送条件（任一满足即可）：批次已满、等待时间达到 linger.ms / retry backoff、BufferPool 内存耗尽、
     * accumulator 已关闭、有线程正在 flush()、事务正在收尾（completing）。
     *
     * @param exhausted 'true' is the buffer pool is exhausted
     *                  BufferPool 是否耗尽且有线程阻塞等待内存；此时所有批次视为 ready，尽快发送以释放内存。
     * @param part The partition
     *             当前检查的分区。
     * @param leader The leader for the partition
     *               该分区当前的 leader broker；ready 时它会被加入 readyNodes。
     * @param waitedTimeMs How long batch waited
     *                     队头批次已在队列中等待的时间。
     * @param backingOff Is backing off
     *                   是否处于重试退避期；退避期内不可发送。
     * @param backoffAttempts Number of attempts for calculating backoff delay
     *                        批次已尝试次数，用于计算指数退避时长。
     * @param full Is batch full
     *             批次是否已满（或队列中还有后续批次）。
     * @param nextReadyCheckDelayMs The delay for next check
     *                              下次 ready 检查的最短等待时间，本方法可能进一步缩小它。
     * @param readyNodes The set of ready nodes (to be filled in)
     *                   可发送数据的目标 broker 集合，本方法会把 leader 加入其中。
     * @return The delay for next check
     *         更新后的下次 ready 检查延迟。
     */
    private long batchReady(boolean exhausted, TopicPartition part, Node leader,
                            long waitedTimeMs, boolean backingOff, int backoffAttempts,
                            boolean full, long nextReadyCheckDelayMs, Set<Node> readyNodes) {
        // 该 broker 已在 ready 集合（一个 broker 只需加一次），或分区被 mute（保序）时，跳过本分区。
        if (!readyNodes.contains(leader) && !isMuted(part)) {
            // 重试批次要等的退避时间随 attempts 指数增长；首次发送的批次只需等到 linger.ms 到期。
            long timeToWaitMs = backingOff ? retryBackoff.backoff(backoffAttempts > 0 ? backoffAttempts - 1 : 0) : lingerMs;
            // 等待时间已超过目标等待时间，说明 linger 或退避已到期。
            boolean expired = waitedTimeMs >= timeToWaitMs;
            // 事务进入 committing/aborting 阶段时，剩余批次也要尽快发出去。
            boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
            boolean sendable = full
                    || expired
                    || exhausted
                    || closed
                    || flushInProgress()
                    || transactionCompleting;
            if (sendable && !backingOff) {
                // 满足发送条件且不在退避期：该分区可以 drain，记录 leader broker。
                readyNodes.add(leader);
            } else {
                // 还差多久可以发送；poll 最多等这么久，到点后回来重新检查。
                long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                // Note that this results in a conservative estimate since an un-sendable partition may have
                // a leader that will later be found to have sendable data. However, this is good enough
                // since we'll just wake up and then sleep again for the remaining time.
                // 这是个保守估计：当前不可发送的分区，其 leader 之后可能因为别的分区而变得可发送；
                // 但这没有关系，因为到点唤醒后只睡掉了剩余时间，随后会重新进入下一轮检查。
                nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
            }
        }
        return nextReadyCheckDelayMs;
    }

    /**
     * Iterate over partitions to see which one have batches ready and collect leaders of those
     * partitions into the set of ready nodes.  If partition has no leader, add the topic to the set
     * of topics with no leader.  This function also calculates stats for adaptive partitioning.
     * 遍历某个 topic 的所有分区队列，逐个判断其队头批次是否可以发送（ready）：
     * <ul>
     * <li>ready 的分区：把 leader broker 收集进 readyNodes，Sender 随后会按 broker drain 这些分区；</li>
     * <li>有数据但 leader 未知的分区：把 topic 收集进 unknownLeaderTopics，Sender 据此触发 metadata 更新；</li>
     * <li>同时为 adaptive partitioning 收集各分区队列长度和 leader rack，供内置分区器评估负载。</li>
     * </ul>
     *
     * @param metadataSnapshot      The cluster metadata
     *                             当前元数据快照，提供分区 leader 与 leader epoch。
     * @param nowMs                 The current time
     *                             当前时间，用于计算批次等待时长和退避。
     * @param topic                 The topic
     *                             当前检查的 topic。
     * @param topicInfo             The topic info
     *                             该 topic 的 TopicInfo，包含分区队列和内置分区器。
     * @param nextReadyCheckDelayMs The delay for next check
     *                             到目前为止下一次 ready 检查的最短等待时间，本方法可能进一步缩小。
     * @param readyNodes            The set of ready nodes (to be filled in)
     *                             可发送数据的目标 broker 集合，本方法会向其填充 leader。
     * @param unknownLeaderTopics   The set of topics with no leader (to be filled in)
     *                             有数据但 leader 未知的 topic 集合，本方法会向其填充 topic。
     * @return The delay for next check
     *         更新后的下一次 ready 检查延迟。
     */
    private long partitionReady(MetadataSnapshot metadataSnapshot, long nowMs, String topic,
                                TopicInfo topicInfo,
                                long nextReadyCheckDelayMs, Set<Node> readyNodes, Set<String> unknownLeaderTopics) {
        // 该 topic 的分区 -> 批次队列映射；map 中可能存在空队列条目（当前不会被清理）。
        ConcurrentMap<Integer, Deque<ProducerBatch>> batches = topicInfo.batches;
        // Collect the queue sizes for available partitions to be used in adaptive partitioning.
        // adaptive partitioning 需要每个可用分区的队列长度来评估负载，仅在该特性开启时收集。
        int[] queueSizes = null;
        int[] partitionIds = null;
        String[] partitionLeaderRacks = null;
        if (enableAdaptivePartitioning && batches.size() >= metadataSnapshot.cluster().partitionsForTopic(topic).size()) {
            // We don't do adaptive partitioning until we scheduled at least a batch for all
            // partitions (i.e. we have the corresponding entries in the batches map), we just
            // do uniform.  The reason is that we build queue sizes from the batches map,
            // and if an entry is missing in the batches map, then adaptive partitioning logic
            // won't know about it and won't switch to it.
            // 只有当每个分区都在 batches map 中有条目（即所有分区都被调度过）时才启用 adaptive。
            // 否则缺失条目的分区在负载统计中不可见，内置分区器永远不会切换到它；此时退化为均匀分发。
            queueSizes = new int[batches.size()];
            partitionIds = new int[queueSizes.length];
            partitionLeaderRacks = new String[queueSizes.length];
        }

        // 已收集统计的可用分区下标；leader 未知或分区被判不可用（回退下标）时不推进。
        int queueSizesIndex = -1;
        // BufferPool 有线程排队等内存时，所有批次都视为 ready，优先把数据发出去释放内存。
        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<Integer, Deque<ProducerBatch>> entry : batches.entrySet()) {
            TopicPartition part = new TopicPartition(topic, entry.getKey());
            // Advance queueSizesIndex so that we properly index available
            // partitions.  Do it here so that it's done for all code paths.
            // 只要 leader 已知且需要统计，就先把下标推进并对齐三个数组（所有代码路径统一在此处理）。
            Node leader = metadataSnapshot.cluster().leaderFor(part);
            if (leader != null && queueSizes != null) {
                ++queueSizesIndex;
                assert queueSizesIndex < queueSizes.length;
                partitionIds[queueSizesIndex] = part.partition();
                partitionLeaderRacks[queueSizesIndex] = leader.rack();
            }

            Deque<ProducerBatch> deque = entry.getValue();

            final long waitedTimeMs;
            final boolean backingOff;
            final int backoffAttempts;
            final int dequeSize;
            final boolean full;

            // 该分区的 leader epoch；重试批次用它和上次尝试比较，判断 leader 是否已经切换。
            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(part);

            // This loop is especially hot with large partition counts. So -
            // 分区数很多时这段循环是热点路径，所以这里特别小心：
            // 1. We should avoid code that increases synchronization between application thread calling
            // send(), and background thread running runOnce(), see https://issues.apache.org/jira/browse/KAFKA-16226
            // 1. 避免增加调用 send() 的应用线程与后台 Sender 线程（runOnce）之间的锁竞争，见 KAFKA-16226；
            // 2. We are careful to only perform the minimum required inside the
            // synchronized block, as this lock is also used to synchronize producer threads
            // attempting to append() to a partition/batch.
            // 2. 锁内只做最少必要的工作——这个 deque 锁同时被 append() 的 producer 线程使用。

            synchronized (deque) {
                // Deques are often empty in this path, esp with large partition counts,
                // so we exit early if we can.
                // 分区数多时队列经常为空；队头没有批次就尽快跳过，减少锁内停留时间。
                ProducerBatch batch = deque.peekFirst();
                if (batch == null) {
                    continue;
                }

                // 队头批次在队列中的等待时长，是 linger / backoff 判断的基础。
                waitedTimeMs = batch.waitedTimeMs(nowMs);
                // 批次若是重试批次，更新其看到的 leader epoch，供判断重试期间 leader 是否变化。
                batch.maybeUpdateLeaderEpoch(leaderEpoch);
                // 重试批次在 retry backoff 到期前不能再次发送。
                backingOff = shouldBackoff(batch.hasLeaderChangedForTheOngoingRetry(), batch, waitedTimeMs);
                // 批次已尝试次数，用于计算指数退避时长。
                backoffAttempts = batch.attempts();
                // 队列长度；>1 表示队头批次之后还有排队批次，即使队头未满也应尽快发送。
                dequeSize = deque.size();
                // 批次已满或队列中还有后续批次时视为 full，可以发送。
                full = dequeSize > 1 || batch.isFull();
            }

            if (leader == null) {
                // This is a partition for which leader is not known, but messages are available to send.
                // Note that entries are currently not removed from batches when deque is empty.
                // 有数据但 leader 未知：记下 topic，Sender 会请求 metadata 更新后再重试。
                // 注意：队列为空时 batches map 中的条目目前不会被移除。
                unknownLeaderTopics.add(part.topic());
            } else {
                if (queueSizes != null)
                    queueSizes[queueSizesIndex] = dequeSize;
                if (partitionAvailabilityTimeoutMs > 0) {
                    // Check if we want to exclude the partition from the list of available partitions
                    // if the broker hasn't responded for some time.
                    // adaptive partitioning 下，若 broker 长时间无法 drain（ready 与 drain 时间差超过阈值），
                    // 说明它可能卡顿，把该分区从可用分区列表中排除，分区器会避开它。
                    NodeLatencyStats nodeLatencyStats = nodeStats.get(leader.id());
                    if (nodeLatencyStats != null) {
                        // NOTE: there is no synchronization between reading metrics,
                        // so we read ready time first to avoid accidentally marking partition
                        // unavailable if we read while the metrics are being updated.
                        // 指标读写之间没有同步：先读 readyTimeMs 再读 drainTimeMs，
                        // 避免恰好在指标更新中间读到不一致的值而误判分区不可用。
                        long readyTimeMs = nodeLatencyStats.readyTimeMs;
                        if (readyTimeMs - nodeLatencyStats.drainTimeMs > partitionAvailabilityTimeoutMs)
                            // 分区被判不可用：回退统计下标，让最后一个已收集的位置可被覆盖/丢弃。
                            --queueSizesIndex;
                    }
                }

                // 该分区的 ready 判定交给 batchReady：满、linger 到期、内存耗尽、flush/close 等条件之一满足即可发送。
                nextReadyCheckDelayMs = batchReady(exhausted, part, leader, waitedTimeMs, backingOff,
                    backoffAttempts, full, nextReadyCheckDelayMs, readyNodes);
            }
        }

        // We've collected the queue sizes for partitions of this topic, now we can calculate
        // load stats.  NOTE: the stats are calculated in place, modifying the
        // queueSizes array.
        // 收集完该 topic 所有分区队列长度后，就地计算负载统计（原地修改 queueSizes 数组），
        // 供 BuiltInPartitioner 在后续 append 选择 sticky 分区时避开高负载分区或慢 broker。
        topicInfo.builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, partitionLeaderRacks, queueSizesIndex + 1);
        return nextReadyCheckDelayMs;
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(MetadataSnapshot metadataSnapshot, long nowMs) {
        // 本轮已经满足发送条件的 broker 集合；Sender 后续会按 broker drain 批次。
        Set<Node> readyNodes = new HashSet<>();
        // 下一次需要重新检查 ready 条件的最短等待时间，通常受 linger.ms 或 retry backoff 影响。
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // 有待发送数据但 leader 未知的 topic，Sender 会据此触发 metadata 更新。
        Set<String> unknownLeaderTopics = new HashSet<>();
        // Go topic by topic so that we can get queue sizes for partitions in a topic and calculate
        // cumulative frequency table (used in partitioner).
        // 按 topic 遍历可同时更新内置分区器的队列统计，并逐分区判断是否 ready。
        for (Map.Entry<String, TopicInfo> topicInfoEntry : this.topicInfoMap.entrySet()) {
            final String topic = topicInfoEntry.getKey();
            nextReadyCheckDelayMs = partitionReady(metadataSnapshot, nowMs, topic, topicInfoEntry.getValue(), nextReadyCheckDelayMs, readyNodes, unknownLeaderTopics);
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                synchronized (deque) {
                    if (!deque.isEmpty())
                        return true;
                }
            }
        }
        return false;
    }

    private boolean shouldBackoff(boolean hasLeaderChanged, final ProducerBatch batch, final long waitedTimeMs) {
        // 重试批次需要等待 retryBackoff；首次发送 attempts=0 时不进入退避。
        boolean shouldWaitMore = batch.attempts() > 0 && waitedTimeMs < retryBackoff.backoff(batch.attempts() - 1);
        // 如果 leader 已变化，可以跳过退避尽快向新 leader 重试。
        boolean shouldBackoff = !hasLeaderChanged && shouldWaitMore;
        if (log.isTraceEnabled()) {
            if (shouldBackoff) {
                log.trace(
                    "For {}, will backoff", batch);
            } else {
                log.trace(
                    "For {}, will not backoff, shouldWaitMore {}, hasLeaderChanged {}", batch,
                    shouldWaitMore, hasLeaderChanged);
            }
        } else if (log.isDebugEnabled() && hasLeaderChanged) {
            // Add less-verbose log at DEBUG.
            log.debug("For {}, leader has changed, hence skipping backoff.", batch);
        }
        return shouldBackoff;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch;
        if (transactionManager != null) {
            // 事务 producer 必须先确认分区已经加入事务，fatal error 时也禁止继续发送。
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            // 幂等/事务发送必须先拿到有效 producerId/epoch。
            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // we cannot send the batch until we have refreshed the producer id
                // 还没有 producerId 时不能 drain，Sender 会优先发送 InitProducerId。
                return true;

            if (!first.hasSequence()) {
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // Don't drain any new batches while the partition has in-flight batches with a different epoch
                    // and/or producer ID. Otherwise, a batch with a new epoch and sequence number
                    // 0 could be written before earlier batches complete, which would cause out of sequence errors
                    // 分区仍有旧 producerId/epoch 的在途批次时，不能发送新 epoch 的批次，否则 broker 可能看到乱序 sequence。
                    return true;
                }

                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // Don't drain any new batches while the state of previous sequence numbers
                    // is unknown. The previous batches would be unknown if they were aborted
                    // on the client after being sent to the broker at least once.
                    // 前序 sequence 是否成功未知时，暂停该分区新批次发送，等待 TransactionManager 解析状态。
                    return true;
            }

            // 找到当前分区最早的 in-flight sequence，用于约束重试批次按 sequence 顺序重新发送。
            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            // If the queued batch already has an assigned sequence, then it is being retried.
            // In this case, we wait until the next immediate batch is ready and drain that.
            // We only move on when the next in line batch is complete (either successfully or due to
            // a fatal broker error). This effectively reduces our in flight request count to 1.
            // 如果队首批次是重试批次，但不是当前最早 in-flight sequence，就停止 drain，避免重试顺序被打乱。
            return firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                    && first.baseSequence() != firstInFlightSequence;
        }
        return false;
    }

    private List<ProducerBatch> drainBatchesForOneNode(MetadataSnapshot metadataSnapshot, Node node, int maxSize, long now) {
        // 本次发往该 broker 的累计请求大小。
        int size = 0;
        // 该 broker 当前作为 leader 的所有分区。
        List<PartitionInfo> parts = metadataSnapshot.cluster().partitionsForNode(node.id());
        // 本轮从 accumulator 中取出的、准备发往该 broker 的批次。
        List<ProducerBatch> ready = new ArrayList<>();
        if (parts.isEmpty())
            return ready;
        /* to make starvation less likely each node has it's own drainIndex */
        /* 每个 broker 独立维护 drainIndex，避免总是从同一个分区开始 drain 导致饥饿。 */
        int drainIndex = getDrainIndex(node.idString());
        int start = drainIndex = drainIndex % parts.size();
        do {
            // 按轮转顺序选择该 broker 的一个 leader 分区。
            PartitionInfo part = parts.get(drainIndex);

            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            // 记录下次从哪个分区继续，保证同一 broker 下分区之间尽量公平。
            updateDrainIndex(node.idString(), drainIndex);
            drainIndex = (drainIndex + 1) % parts.size();
            // Only proceed if the partition has no in-flight batches.
            // muted 分区通常表示有前序批次未完成，跳过以保持顺序。
            if (isMuted(tp))
                continue;
            // 获取该分区的待发送批次队列。
            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            // 记录当前 metadata 中的 leader epoch，批次重试时会用它判断 leader 是否变化。
            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(tp);

            final ProducerBatch batch;
            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                // 只查看队首批次；同一分区必须按 append 顺序发送。
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // first != null
                // Only drain the batch if it is not during backoff period.
                // 更新批次看到的 leader epoch，并判断重试退避是否已经结束。
                first.maybeUpdateLeaderEpoch(leaderEpoch);
                if (shouldBackoff(first.hasLeaderChangedForTheOngoingRetry(), first, first.waitedTimeMs(now)))
                    continue;

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size due to
                    // compression; in this case we will still eventually send this batch in a single request
                    // 当前请求已接近 max.request.size，且已有批次可发，则停止继续加入更多批次。
                    break;
                } else {
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        // 幂等/事务/顺序条件不允许发送该分区时，停止本 broker 本轮 drain。
                        break;
                }

                // 真正从分区队列头部移除，交给 Sender 构造 ProduceRequest。
                batch = deque.pollFirst();

                // 判断当前批次是否属于事务发送；该状态会写入 RecordBatch header。
                boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                ProducerIdAndEpoch producerIdAndEpoch =
                    transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                if (producerIdAndEpoch != null && !batch.hasSequence()) {
                    // If the producer id/epoch of the partition do not match the latest one
                    // of the producer, we update it and reset the sequence. This should be
                    // only done when all its in-flight batches have completed. This is guarantee
                    // in `shouldStopDrainBatchesForPartition`.
                    transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                    // If the batch already has an assigned sequence, then we should not change the producer id and
                    // sequence number, since this may introduce duplicates. In particular, the previous attempt
                    // may actually have been accepted, and if we change the producer id and sequence here, this
                    // attempt will also be accepted, causing a duplicate.
                    //
                    // Additionally, we update the next sequence number bound for the partition, and also have
                    // the transaction manager track the batch so as to ensure that sequence ordering is maintained
                    // even if we receive out of order responses.
                    // 给首次发送的批次分配 producerId/epoch/baseSequence，并写入批次 header。
                    batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                    // 推进该分区下一批次应使用的 sequence。
                    transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                    log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                            "{} being sent to partition {}", producerIdAndEpoch.producerId,
                        producerIdAndEpoch.epoch, batch.baseSequence(), tp);

                    // 事务管理器开始跟踪该在途批次，用于响应乱序、失败重试和 sequence 恢复。
                    transactionManager.addInFlightBatch(batch);
                }
            }

            // the rest of the work by processing outside the lock
            // close() is particularly expensive
            // 离开 deque 锁后再 close batch，避免压缩收尾等较重操作阻塞 append 线程。
            batch.close();
            // 统计本次请求已经聚合的 records 字节数。
            size += batch.records().sizeInBytes();
            ready.add(batch);

            // 记录 batch 被 drain 的时间，用于 queue-time 指标。
            batch.drained(now);
        } while (start != drainIndex);
        return ready;
    }

    private int getDrainIndex(String idString) {
        return nodesDrainIndex.computeIfAbsent(idString, s -> 0);
    }

    private void updateDrainIndex(String idString, int drainIndex) {
        nodesDrainIndex.put(idString, drainIndex);
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit
     * within the specified size on a per-node basis. This method attempts to avoid choosing the same
     * topic-node over and over.
     *
     * @param metadataSnapshot  The current cluster metadata
     * @param nodes             The list of node to drain
     * @param maxSize           The maximum number of bytes to drain
     * @param now               The current unix time in milliseconds
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the
     * requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(MetadataSnapshot metadataSnapshot, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        // 返回结构以 brokerId 为 key，value 是本轮发往该 broker 的批次列表。
        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            // 每个 broker 独立 drain，生成一个 ProduceRequest 的候选批次集合。
            List<ProducerBatch> ready = drainBatchesForOneNode(metadataSnapshot, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    public void updateNodeLatencyStats(Integer nodeId, long nowMs, boolean canDrain) {
        // Don't bother with updating stats if the feature is turned off.
        if (partitionAvailabilityTimeoutMs <= 0)
            return;

        // When the sender gets a node (returned by the ready() function) that has data to send
        // but the node is not ready (and so we cannot drain the data), we only update the
        // ready time, then the difference would reflect for how long a node wasn't ready
        // to send the data.  Then we can temporarily remove partitions that are handled by the
        // node from the list of available partitions so that the partitioner wouldn't pick
        // this partition.
        // NOTE: there is no synchronization for metric updates, so drainTimeMs is updated
        // first to avoid accidentally marking a partition unavailable if the reader gets
        // values between updates.
        NodeLatencyStats nodeLatencyStats = nodeStats.computeIfAbsent(nodeId, id -> new NodeLatencyStats(nowMs));
        if (canDrain)
            nodeLatencyStats.drainTimeMs = nowMs;
        nodeLatencyStats.readyTimeMs = nowMs;
    }

    /* Visible for testing */
    public NodeLatencyStats getNodeLatencyStats(Integer nodeId) {
        return nodeStats.get(nodeId);
    }

    /* Visible for testing */
    public BuiltInPartitioner getBuiltInPartitioner(String topic) {
        return topicInfoMap.get(topic).builtInPartitioner;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

      /* Visible for testing */
    public Deque<ProducerBatch> getDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.get(tp.topic());
        if (topicInfo == null)
            return null;
        return topicInfo.batches.get(tp.partition());
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(tp.topic(),
                k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize, partitionerRackAware, rack)));
        return topicInfo.batches.computeIfAbsent(tp.partition(), k -> new ArrayDeque<>());
    }

    BuiltInPartitioner createBuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize, boolean rackAware, String rack) {
        return new BuiltInPartitioner(logContext, topic, stickyBatchSize, rackAware, rack);
    }

    /**
     * Complete and deallocate the record batch
     */
    public void completeAndDeallocateBatch(ProducerBatch batch) {
        // 先从 incomplete 集合移除，再释放 ByteBuffer 回 BufferPool。
        completeBatch(batch);
        deallocate(batch);
    }

    /**
     * Only perform deallocation (and not removal from the incomplete set)
     */
    public void deallocate(ProducerBatch batch) {
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        if (!batch.isSplitBatch()) {
            if (batch.isBufferDeallocated()) {
                log.warn("Skipping deallocating a batch that has already been deallocated. Batch is {}, created time is {}", batch, batch.createdMs);
            } else {
                batch.markBufferDeallocated();
                if (batch.isInflight()) {
                    // Create a fresh ByteBuffer to give to BufferPool to reuse since we can't safely call deallocate with the ProduceBatch's buffer
                    free.deallocate(ByteBuffer.allocate(batch.initialCapacity()));
                    throw new IllegalStateException("Attempting to deallocate a batch that is inflight. Batch is " + batch);
                }
                free.deallocate(batch.buffer(), batch.initialCapacity());
            }
        }
    }

    /**
     * Remove from the incomplete list but do not free memory yet
     */
    public void completeBatch(ProducerBatch batch) {
        // 只移除 incomplete 标记，缓冲区可能仍由网络层持有，释放由调用方决定。
        incomplete.remove(batch);
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the time of the flush.
            // We must be careful not to hold a reference to the ProduceBatch(s) so that garbage
            // collection can occur on the contents.
            // The sender will remove ProducerBatch(s) from the original incomplete collection.
            //
            // We use awaitAllDependents() here instead of await() to ensure that if any batch
            // was split into multiple batches, we wait for all the split batches to complete.
            // This is required to guarantee that all records sent before flush()
            // must be fully complete, including records in split batches.
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.awaitAllDependents();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.topicInfoMap.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            // 找到批次所在分区队列，把未发送批次从队列中移除。
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            synchronized (dq) {
                // 先终止继续 append，避免在持锁场景下触发用户 callback。
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            // 失败 batch 中所有 record 的 Future/callback。
            batch.abort(reason);
            if (batch.isInflight()) {
                // KAFKA-19012: if the batch has been sent it might still be in use by the network client so we cannot allow it to be reused yet.
                // We skip deallocating it now. When the request in network client completes with a response, either Sender.completeBatch() or
                // Sender.failBatch() will be called with deallocateBatch=true. The buffer associated with the batch will be deallocated then.
                // in-flight 批次可能仍被 NetworkClient 引用，只能先移除 incomplete，稍后由 Sender 释放。
                completeBatch(batch);
            } else {
                // 未发送批次可以立即从 incomplete 移除并释放缓冲区。
                completeAndDeallocateBatch(batch);
            }
        }
    }

    /**
     * Abort any batches which have not been drained
     */
    void abortUndrainedBatches(RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {
                // 只中止尚未 drain 的批次：事务批次未分配 sequence，非事务批次还没 close。
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();
                    dq.remove(batch);
                }
            }
            if (aborted) {
                // 对未 drain 批次可以立即完成异常并释放内存。
                batch.abort(reason);
                completeAndDeallocateBatch(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        // 暂停该分区继续 drain，通常用于保证 max.in.flight=1 或事务/幂等顺序。
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        // 当前分区前序批次完成后解除暂停，允许后续批次继续发送。
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
        this.free.close();
    }

    /**
     * Partitioner config for built-in partitioner
     */
    public static final class PartitionerConfig {
        private final boolean enableAdaptivePartitioning;
        private final long partitionAvailabilityTimeoutMs;
        private final boolean rackAware;
        private final String rack;

        /**
         * Partitioner config
         *
         * @param enableAdaptivePartitioning If it's true, partition switching adapts to broker load, otherwise partition
         *        switching is random.
         * @param partitionAvailabilityTimeoutMs If a broker cannot process produce requests from a partition
         *        for the specified time, the partition is treated by the partitioner as not available.
         *        If the timeout is 0, this logic is disabled.
         * @param rackAware Whether the built-in partitioner is configured to be rack-aware.
         * @param rack The producer rack.
         */
        public PartitionerConfig(boolean enableAdaptivePartitioning, long partitionAvailabilityTimeoutMs, boolean rackAware, String rack) {
            this.enableAdaptivePartitioning = enableAdaptivePartitioning;
            this.partitionAvailabilityTimeoutMs = partitionAvailabilityTimeoutMs;
            this.rackAware = rackAware;
            this.rack = rack;

            if (rackAware && Utils.isBlank(rack)) {
                throw new ConfigException("client.rack must be provided if partitioner.rack.aware is enabled");
            }
        }

        public PartitionerConfig() {
            this(false, 0, false, "");
        }
    }

    /*
     * Metadata about a record just appended to the record accumulator
     * 刚追加到 RecordAccumulator 的单条记录结果。
     * KafkaProducer#doSend 依赖这里判断是否唤醒 Sender，并把 future 返回给用户。
     */
    public static final class RecordAppendResult {
        // 当前 record 对应的 Future；broker ack 或发送失败后会完成。
        public final FutureRecordMetadata future;
        // 追加后该 batch 是否已满；为 true 时 doSend 会唤醒 Sender 尽快发送。
        public final boolean batchIsFull;
        // 本次追加是否创建了新 batch；新 batch 也会触发 Sender 唤醒。
        public final boolean newBatchCreated;
        // 本次追加带来的 batch 估算字节增长，用于内置分区器判断 sticky 分区是否应切换。
        public final int appendedBytes;

        /**
         * @param future 当前 record 对应的异步结果
         * @param batchIsFull 追加后 batch 是否已满或队列中是否有可发送批次
         * @param newBatchCreated 本次追加是否新建了 ProducerBatch
         * @param appendedBytes 本次追加增加的估算字节数
         */
        public RecordAppendResult(FutureRecordMetadata future,
                                  boolean batchIsFull,
                                  boolean newBatchCreated,
                                  int appendedBytes) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.appendedBytes = appendedBytes;
        }
    }

    /*
     * The callbacks passed into append
     * 传入 append 的回调扩展。
     * 除了普通 Callback#onCompletion 外，还允许 accumulator 在确定真实分区后回填 partition。
     */
    public interface AppendCallbacks extends Callback {
        /**
         * Called to set partition (when append is called, partition may not be calculated yet).
         * @param partition The partition
         * 当 KafkaProducer#doSend 传入 UNKNOWN_PARTITION 时，真实分区会在 append 内部确定并通过这里回填。
         */
        void setPartition(int partition);
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     * ready() 的检查结果：告诉 Sender 哪些 broker 有可发送数据、多久后再检查、哪些 topic leader 未知。
     */
    public static final class ReadyCheckResult {
        // 至少有一个分区 batch 满足发送条件的 broker 集合。
        public final Set<Node> readyNodes;
        // 下一次应该重新检查 ready 条件的延迟，通常由 linger.ms 或 retry backoff 决定。
        public final long nextReadyCheckDelayMs;
        // 有待发送数据但当前 metadata 中找不到 leader 的 topic。
        public final Set<String> unknownLeaderTopics;

        /**
         * @param readyNodes 可发送数据所在的 broker 集合
         * @param nextReadyCheckDelayMs 下一次 ready 检查的最短等待时间
         * @param unknownLeaderTopics leader 未知但存在待发送数据的 topic 集合
         */
        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }

    /**
     * Per topic info.
     * 每个 topic 的本地累加状态，包括分区 batch 队列和该 topic 的内置分区器。
     */
    private static class TopicInfo {
        // key 是 partition，value 是该分区按追加顺序排列的 ProducerBatch 队列。
        public final ConcurrentMap<Integer /*partition*/, Deque<ProducerBatch>> batches = new CopyOnWriteMap<>();
        // 该 topic 的 sticky/adaptive 分区器，用于 UNKNOWN_PARTITION 场景选择真实分区。
        public final BuiltInPartitioner builtInPartitioner;

        /**
         * @param builtInPartitioner 当前 topic 使用的内置分区器
         */
        public TopicInfo(BuiltInPartitioner builtInPartitioner) {
            this.builtInPartitioner = builtInPartitioner;
        }
    }

    /**
     * Node latency stats for each node that are used for adaptive partition distribution
     * Visible for testing
     */
    public static final class NodeLatencyStats {
        public volatile long readyTimeMs;  // last time the node had batches ready to send
        public volatile long drainTimeMs;  // last time the node was able to drain batches

        NodeLatencyStats(long nowMs) {
            readyTimeMs = nowMs;
            drainTimeMs = nowMs;
        }
    }
}
