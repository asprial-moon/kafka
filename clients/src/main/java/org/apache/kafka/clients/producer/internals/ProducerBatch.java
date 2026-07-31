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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.AbstractRecords;
import org.apache.kafka.common.record.internal.CompressionRatioEstimator;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.ProducerIdAndEpoch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.common.record.internal.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.internal.RecordBatch.NO_TIMESTAMP;

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    // batch 的最终状态：中止、失败或成功。null 表示尚未完成。
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }

    // batch 创建时间，用于 delivery.timeout.ms、queue time、等待时间等计算。
    final long createdMs;
    // 当前 batch 归属的 topic-partition；一个 ProducerBatch 只属于一个分区。
    final TopicPartition topicPartition;
    // batch 级异步结果，batch 内每条记录的 FutureRecordMetadata 都依赖它完成。
    final ProduceRequestResult produceFuture;

    // batch 内每条 record 对应的 callback/future 绑定关系，完成 batch 时逐个触发。
    private final List<Thunk> thunks = new ArrayList<>();
    // 真正写入 record 二进制数据的构建器，底层持有 ByteBuffer。
    private final MemoryRecordsBuilder recordsBuilder;
    // 已发送尝试次数；重试入队时递增。
    private final AtomicInteger attempts = new AtomicInteger(0);
    // 标记该 batch 是否由大批次拆分而来；拆分批次的内存不从 BufferPool 分配。
    private final boolean isSplitBatch;
    // batch 最终状态的原子引用，保证成功/失败/中止只会有一个最终结果生效。
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);
    // 当前 batch 底层 buffer 是否已经归还或标记释放，避免重复释放。
    private boolean bufferDeallocated = false;
    // Tracks if the batch has been sent to the NetworkClient
    // 是否已经交给 NetworkClient 发送；in-flight batch 的 buffer 不能被立即复用。
    private boolean inflight = false;

    // batch 内 record 数量，同时也是下一条 record 的 batchIndex。
    int recordCount;
    // batch 内最大单条 record 的估算大小，用于指标和大批次拆分判断。
    int maxRecordSize;
    // 最近一次发送尝试时间，用于 retry backoff 等等待时间计算。
    private long lastAttemptMs;
    // 最近一次 append record 的时间，用于 linger.ms、过期和 ready 判断。
    private long lastAppendTime;
    // batch 被 Sender 从 accumulator drain 出来的时间，用于 queue-time 指标。
    private long drainedMs;
    // 标记该 batch 是否处于重试流程。
    private boolean retry;
    // 标记 batch 是否曾因 producerId/epoch/sequence 重写而重新打开。
    private boolean reopened;

    // Tracks the current-leader's epoch to which this batch would be sent, in the current to produce the batch.
    // 当前尝试发送时目标 leader 的 epoch，用于判断重试期间 leader 是否发生变化。
    private OptionalInt currentLeaderEpoch;
    // Tracks the attempt in which leader was changed to currentLeaderEpoch for the 1st time.
    // 第一次观察到 currentLeaderEpoch 变化时的发送尝试次数，用于跳过不必要的 retry backoff。
    private int attemptsWhenLeaderLastChanged;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compression().type());
        this.currentLeaderEpoch = OptionalInt.empty();
        this.attemptsWhenLeaderLastChanged = 0;
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * It will update the leader to which this batch will be produced for the ongoing attempt, if a newer leader is known.
     * @param latestLeaderEpoch latest leader's epoch.
     */
    void maybeUpdateLeaderEpoch(OptionalInt latestLeaderEpoch) {
        if (latestLeaderEpoch.isPresent()
            && (currentLeaderEpoch.isEmpty() || currentLeaderEpoch.getAsInt() < latestLeaderEpoch.getAsInt())) {
            log.trace("For {}, leader will be updated, currentLeaderEpoch: {}, attemptsWhenLeaderLastChanged:{}, latestLeaderEpoch: {}, current attempt: {}",
                this, currentLeaderEpoch, attemptsWhenLeaderLastChanged, latestLeaderEpoch, attempts);
            attemptsWhenLeaderLastChanged = attempts();
            currentLeaderEpoch = latestLeaderEpoch;
        } else {
            log.trace("For {}, leader wasn't updated, currentLeaderEpoch: {}, attemptsWhenLeaderLastChanged:{}, latestLeaderEpoch: {}, current attempt: {}",
                this, currentLeaderEpoch, attemptsWhenLeaderLastChanged, latestLeaderEpoch, attempts);
        }
    }

    /**
     * It will return true, for a when batch is being retried, it will be retried to a newer leader.
     */

    boolean hasLeaderChangedForTheOngoingRetry() {
        int attempts = attempts();
        boolean isRetry = attempts >= 1;
        if (!isRetry)
            return false;
        return attempts == attemptsWhenLeaderLastChanged;
    }


    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 将单条记录追加到当前 ProducerBatch，并返回该记录对应的 FutureRecordMetadata。
     *
     * @param timestamp 当前记录时间戳
     * @param key 当前记录 key 字节数组，可能为 null
     * @param value 当前记录 value 字节数组，可能为 null
     * @param headers 当前记录 headers
     * @param callback 当前记录完成时要触发的 callback
     * @param now 当前追加时间，用于更新 batch 的 lastAppendTime
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     *         返回当前记录的 FutureRecordMetadata；如果当前 batch 空间不足则返回 null。
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 当前批次没有足够空间时返回 null，让 RecordAccumulator 创建新批次。
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 真正把序列化后的 key/value/header 写入 MemoryRecordsBuilder。
            this.recordsBuilder.append(timestamp, key, value, headers);
            // 维护本批次内单条记录的最大估算大小，用于后续大批次拆分等逻辑。
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compression().type(), key, value, headers));
            // 更新最后追加时间，用于 linger、超时和 ready 判断。
            this.lastAppendTime = now;
            // 为本条记录创建 Future。batchIndex 是它在当前批次内的相对位置，
            // broker 返回 baseOffset 后可通过 baseOffset + batchIndex 推导该记录 offset。
            // key/value 长度会进入 RecordMetadata，null 用 -1 表示。
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            // 保存 callback 与 future 的绑定关系，批次完成时会逐个触发 callback。
            // 即使后续批次被拆分重试，也能通过 future chain 继续等待新批次结果。
            thunks.add(new Thunk(callback, future));
            // recordCount 同时是下一条消息的 batchIndex。
            this.recordCount++;
            return future;
        }
    }

    /**
     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     * 仅在大批次拆分时使用：把原批次中的 record 追加到拆分后的新批次。
     *
     * @param timestamp 原 record 时间戳
     * @param key 原 record key buffer
     * @param value 原 record value buffer
     * @param headers 原 record headers
     * @param thunk 原 record 的 callback/future 绑定关系，需要迁移到拆分后的批次
     * @return true if the record has been successfully appended, false otherwise.
     *         追加成功返回 true；新拆分批次空间不足返回 false。
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        // 拆分后的目标 batch 空间不足时，让调用方创建下一个拆分 batch。
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // No need to get the CRC.
            // 拆分场景直接把原 record 内容写入新 batch。
            this.recordsBuilder.append(timestamp, key, value, headers);
            // 更新拆分批次中的最大单条记录大小。
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compression().type(), key, value, headers));
            // 为拆分后的批次创建新的 FutureRecordMetadata，并保持 batchIndex 与新批次内位置一致。
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // Chain the future to the original thunk.
            // 原用户 Future 需要链到拆分后的新 Future，确保用户等待的是拆分后真正发送的结果。
            thunk.future.chain(future);
            // 复用原 callback 绑定关系，拆分后仍能触发同一个用户 callback。
            this.thunks.add(thunk);
            // 推进拆分批次内下一条 record 的 batchIndex。
            this.recordCount++;
            return true;
        }
    }

    /**
     * Abort the batch and complete the future and callbacks.
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    public void abort(RuntimeException exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, index -> exception);
    }

    /**
     * Check if the batch has been completed (either successfully or exceptionally).
     * 判断批次是否已经进入最终状态；成功、失败、abort 都算完成。
     * @return `true` if the batch has been completed, `false` otherwise.
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * Complete the batch successfully.
     * 成功完成批次：broker 已返回成功响应，baseOffset/logAppendTime 会写入每条 record 的 Future。
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @return true if the batch was completed as a result of this call, and false
     *   if it had been completed previously
     */
    public boolean complete(long baseOffset, long logAppendTime) {
        // 成功路径没有异常映射，所有 record 都会得到 RecordMetadata。
        return done(baseOffset, logAppendTime, null, null);
    }

    /**
     * Complete the batch exceptionally. The provided top-level exception will be used
     * for each record future contained in the batch.
     * 异常完成批次：Sender 失败或 broker 返回错误时调用，batch 内 record 会按异常映射完成失败。
     *
     * @param topLevelException top-level partition error
     * @param recordExceptions Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed as a result of this call, and false
     *   if it had been completed previously
     */
    public boolean completeExceptionally(
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        Objects.requireNonNull(topLevelException);
        Objects.requireNonNull(recordExceptions);
        // 失败路径没有有效 offset/timestamp，使用协议中的无效占位值。
        return done(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, topLevelException, recordExceptions);
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be called
     * once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     * state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
     * try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
     * ABORTED but again it could succeed if broker responds with a success.
     *
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     * Attempted transitions from one failure state to the same or a different failed state are ignored.
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param topLevelException The exception that occurred (or null if the request was successful)
     * @param recordExceptions Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     */
    private boolean done(
        long baseOffset,
        long logAppendTime,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        // topLevelException 为空表示成功，否则表示失败；abort 会通过其他路径设置 ABORTED。
        final FinalState tryFinalState = (topLevelException == null) ? FinalState.SUCCEEDED : FinalState.FAILED;
        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, topLevelException);
        }

        // 只有第一个完成者能设置最终状态，并真正触发 Future/callback。
        if (this.finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, recordExceptions);
            return true;
        }

        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                // 客户端先判定失败/abort，但 broker 后来返回成功；只记录，不重复触发 callback。
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                // 多条失败路径竞争完成同一批次时忽略后来的失败状态。
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // A SUCCESSFUL batch must not attempt another state change.
            // 已成功的批次不能再转失败，否则会破坏用户已观察到的发送结果。
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    private void completeFutureAndFireCallbacks(
        long baseOffset,
        long logAppendTime,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        // 先设置 Future 结果，再执行 callback；callback 中可能读取 Future 状态。
        produceFuture.set(baseOffset, logAppendTime, recordExceptions);

        // execute callbacks
        // 逐条 record 触发对应 callback；一个 batch 可能包含多条 record。
        for (int i = 0; i < thunks.size(); i++) {
            try {
                // thunk 保存单条 record 的 callback 和 FutureRecordMetadata。
                Thunk thunk = thunks.get(i);
                if (thunk.callback != null) {
                    if (recordExceptions == null) {
                        // 成功路径通过 FutureRecordMetadata 生成 RecordMetadata。
                        RecordMetadata metadata = thunk.future.value();
                        thunk.callback.onCompletion(metadata, null);
                    } else {
                        // 失败路径按 batchIndex 获取该 record 对应的异常。
                        RuntimeException exception = recordExceptions.apply(i);
                        thunk.callback.onCompletion(null, exception);
                    }
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 标记 ProduceRequestResult 完成，唤醒等待 Future.get()/flush 的线程。
        produceFuture.done();
    }

    public Deque<ProducerBatch> split(int splitBatchSize) {
        RecordBatch recordBatch = validateAndGetRecordBatch();
        Deque<ProducerBatch> batches = splitRecordsIntoBatches(recordBatch, splitBatchSize);
        finalizeSplitBatches(batches);
        return batches;
    }

    private RecordBatch validateAndGetRecordBatch() {
        MemoryRecords memoryRecords = recordsBuilder.build();
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();

        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        return recordBatch;
    }

    private Deque<ProducerBatch> splitRecordsIntoBatches(RecordBatch recordBatch, int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        ProducerBatch batch = null;

        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            Thunk thunk = thunkIter.next();
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // A newly created batch can always host the first message.
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                batch.closeForRecordAppends();
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // Close the last batch and add it to the batch list after split.
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        return batches;
    }

    private void finalizeSplitBatches(Deque<ProducerBatch> batches) {
        // Chain all split batch ProduceRequestResults to the original batch's produceFuture
        // Ensures the original batch's future doesn't complete until all split batches complete
        for (ProducerBatch splitBatch : batches) {
            produceFuture.addDependent(splitBatch.produceFuture);
        }

        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, index -> new RecordBatchTooLargeException());
        produceFuture.done();

        assignProducerStateToBatches(batches);
    }

    private void assignProducerStateToBatches(Deque<ProducerBatch> batches) {
        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
    }

    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compression().type(), record.key(), record.value(), record.headers()), batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
        // for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
        // with how normal batches are handled).
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compression(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compression().type() != CompressionType.NONE;
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     * 单条记录的回调与 Future 组合。
     * ProducerBatch 完成时会遍历 thunks，用 future 生成 RecordMetadata 并调用 callback。
     */
    private static final class Thunk {
        // KafkaProducer#doSend 传入的回调适配器，最终会触发拦截器和用户 callback。
        final Callback callback;
        // 当前记录的异步结果句柄。
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        // 从 batch 创建时间开始计算交付超时，覆盖排队、发送、重试和等待响应全过程。
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        // 已尝试发送次数；首次发送前为 0，重试入队时递增。
        return attempts.get();
    }

    void reenqueued(long now) {
        // 批次重新入队表示一次发送尝试已经失败，增加 attempts 并刷新重试等待起点。
        attempts.getAndIncrement();
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        // 标记该 batch 处于重试状态，Sender/TransactionManager 会据此处理 sequence 和超时。
        retry = true;
    }

    long queueTimeMs() {
        // batch 从创建到被 Sender drain 的排队时间。
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        // 距离上次发送尝试已经等待多久，用于 retryBackoff 判断。
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        // 记录 batch 被 accumulator drain 出来的时间。
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     * 判断该 batch 是否已经经历过发送失败并重新入队。
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        // 构建并返回底层 MemoryRecords，Sender 会把它直接放入 ProduceRequest。
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        // 在 batch header 中写入 producerId、epoch、baseSequence 和事务标记。
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional());
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        // 关闭 recordsBuilder，完成压缩/校验和等收尾，之后不能继续 append record。
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compression().type(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to aborting
     * the batch with {@link #abort(RuntimeException)} and ensures that no record previously appended can be
     * read. This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in which
     * it is not safe to invoke the completion callbacks (e.g. because we are holding a lock, such as
     * when aborting batches in {@link RecordAccumulator}).
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        // baseSequence 不是 NO_SEQUENCE 表示该批次已被分配幂等序列号。
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }

    public boolean isBufferDeallocated() {
        return bufferDeallocated;
    }

    public void markBufferDeallocated() {
        bufferDeallocated = true;
    }

    public boolean isInflight() {
        // batch 是否已经进入 NetworkClient 发送链路但尚未最终完成。
        return inflight;
    }

    public void setInflight(boolean inflight) {
        // Sender 构造 ProduceRequest 时置为 true，响应完成或失败处理时再置回 false。
        this.inflight = inflight;
    }

    // VisibleForTesting
    OptionalInt currentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    // VisibleForTesting
    int attemptsWhenLeaderLastChanged() {
        return attemptsWhenLeaderLastChanged;
    }
}
