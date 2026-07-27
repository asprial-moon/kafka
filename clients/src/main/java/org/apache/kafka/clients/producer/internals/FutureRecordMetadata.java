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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future result of a record send
 * 单条 ProducerRecord 发送结果的 Future。
 * KafkaProducer#doSend 成功把记录追加到 RecordAccumulator 后，返回的就是该对象。
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    // 当前 record 所在 ProducerBatch 对应的共享请求结果；同一批次内多条 record 共用它。
    private final ProduceRequestResult result;
    // 当前 record 在批次内的相对下标，用于通过 baseOffset + batchIndex 计算最终 offset。
    private final int batchIndex;
    // record 创建时间；broker 未使用 LogAppendTime 时作为最终 metadata timestamp。
    private final long createTimestamp;
    // 序列化后 key 的字节数；key 为 null 时为 -1。
    private final int serializedKeySize;
    // 序列化后 value 的字节数；value 为 null 时为 -1。
    private final int serializedValueSize;
    // 时间抽象，用于带超时的 get 计算剩余等待时间。
    private final Time time;
    // 大批次被拆分重试时，原 Future 链接到拆分后新批次的 Future，保证用户仍等待最终结果。
    private volatile FutureRecordMetadata nextRecordMetadata = null;

    public FutureRecordMetadata(ProduceRequestResult result, int batchIndex, long createTimestamp, int serializedKeySize,
                                int serializedValueSize, Time time) {
        this.result = result;
        this.batchIndex = batchIndex;
        this.createTimestamp = createTimestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.time = time;
    }

    /**
     * Attempt to cancel the send future.
     * 尝试取消发送 Future；Kafka Producer 的发送 Future 不支持取消，因此始终返回 false。
     */
    @Override
    public boolean cancel(boolean interrupt) {
        // Kafka Producer 的发送 Future 不支持取消；记录一旦进入 batch，就由 Sender 统一处理。
        return false;
    }

    /**
     * Return whether this future was cancelled.
     * 返回该 Future 是否已取消；由于不支持取消，因此始终为 false。
     */
    @Override
    public boolean isCancelled() {
        // 不支持取消，因此永远返回 false。
        return false;
    }

    /**
     * Wait for the send result and return record metadata.
     * 等待发送完成并返回 RecordMetadata；如果发送失败，则按 Future 语义抛出 ExecutionException。
     */
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        // 等待当前批次的 ProduceRequest 完成。
        this.result.await();
        if (nextRecordMetadata != null)
            // 如果批次被拆分，递归等待拆分后新批次的结果。
            return nextRecordMetadata.get();
        // 批次完成后，要么返回 RecordMetadata，要么抛出发送异常。
        return valueOrError();
    }

    /**
     * Wait for the send result up to the given timeout.
     * 在指定超时时间内等待发送完成；超时则抛出 TimeoutException。
     */
    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Handle overflow.
        long now = time.milliseconds();
        long timeoutMillis = unit.toMillis(timeout);
        long deadline = Long.MAX_VALUE - timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
        // 带超时等待当前批次完成。
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.");
        if (nextRecordMetadata != null)
            // 如果存在链式 Future，把剩余时间继续传给下一个 Future。
            return nextRecordMetadata.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS);
        return valueOrError();
    }

    /**
     * This method is used when we have to split a large batch in smaller ones. A chained metadata will allow the
     * future that has already returned to the users to wait on the newly created split batches even after the
     * old big batch has been deemed as done.
     * 当大批次需要拆分成多个小批次重试时使用该方法。
     * 通过链式 Future，已经返回给用户的 Future 仍然可以继续等待拆分后新批次的最终发送结果。
     */
    void chain(FutureRecordMetadata futureRecordMetadata) {
        // 大批次拆分可能多次发生，因此链表式挂接新的 Future。
        if (nextRecordMetadata == null)
            nextRecordMetadata = futureRecordMetadata;
        else
            nextRecordMetadata.chain(futureRecordMetadata);
    }

    /**
     * Return metadata or throw the record-level send error.
     * 如果当前记录发送成功则返回元数据；如果失败则抛出记录级发送异常。
     */
    RecordMetadata valueOrError() throws ExecutionException {
        // ProduceRequestResult 按 batchIndex 保存单条记录错误；有异常则按 Future 规范包装抛出。
        RuntimeException exception = this.result.error(batchIndex);
        if (exception != null)
            throw new ExecutionException(exception);
        else
            return value();
    }

    /**
     * Build RecordMetadata from the completed produce request result.
     * 根据已完成的 ProduceRequestResult 构造当前记录的 RecordMetadata。
     */
    RecordMetadata value() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.value();
        // broker 返回 baseOffset 后，当前记录的实际 offset = baseOffset + batchIndex。
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.batchIndex,
                                  timestamp(), this.serializedKeySize, this.serializedValueSize);
    }

    /**
     * Resolve the timestamp to expose in RecordMetadata.
     * 解析 RecordMetadata 中应暴露的时间戳，优先使用 broker 日志追加时间。
     */
    private long timestamp() {
        // broker 配置 LogAppendTime 时以 broker 日志追加时间为准，否则使用 record 创建时间。
        return result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp;
    }

    /**
     * Return whether the send result is complete.
     * 返回发送结果是否已经完成；批次拆分时以链上最终 Future 为准。
     */
    @Override
    public boolean isDone() {
        if (nextRecordMetadata != null)
            // 链式 Future 场景下，以最终新批次是否完成为准。
            return nextRecordMetadata.isDone();
        return this.result.completed();
    }

}
