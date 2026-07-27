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

package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.internal.AbstractRecords;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.internals.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("linger.ms", 1);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called, it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The default setting "all"
 * will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry. The <code>retries</code> setting defaults to <code>Integer.MAX_VALUE</code>, and
 * it's recommended to use <code>delivery.timeout.ms</code> to control retry behavior, instead of <code>retries</code>.
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code>. So, under heavy load,
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it returns
 * a failed future with BufferExhaustedException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple byte or string types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * </p>
 * <p>
 * From Kafka 3.0, the <code>enable.idempotence</code> configuration defaults to true. When enabling idempotence,
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * As such, it should be unique to each producer instance running within a partitioned application.
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 * 100 messages are part of a single transaction.
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * </p>
 */
@InterfaceAudience.Public
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private static final String INIT_TXN_TIMEOUT_MSG = "InitTransactions timed out - " +
            "did not complete coordinator discovery or " +
            "receive the InitProducerId response within max.block.ms.";

    private static final String SEND_OFFSETS_TIMEOUT_MSG =
            "SendOffsetsToTransaction timed out - did not reach the coordinator or " +
                    "receive the TxnOffsetCommit/AddOffsetsToTxn response within max.block.ms";
    private static final String COMMIT_TXN_TIMEOUT_MSG =
            "CommitTransaction timed out - did not complete EndTxn with the transaction coordinator within max.block.ms";
    private static final String ABORT_TXN_TIMEOUT_MSG =
            "AbortTransaction timed out - did not complete EndTxn(abort) with the transaction coordinator within max.block.ms";
    
    // 当前 Producer 实例的客户端标识，会写入日志、指标和请求上下文，便于定位发送端。
    private final String clientId;
    // Visible for testing
    // Producer 级别指标容器，doSend 中的错误计数和元数据等待耗时都会记录到这里。
    final Metrics metrics;
    // Producer 专用指标封装，负责记录发送链路中的关键耗时和状态。
    private final KafkaProducerMetrics producerMetrics;
    // 用户配置的自定义分区器插件；为空时使用 Kafka 内置分区逻辑。
    private final Plugin<Partitioner> partitionerPlugin;
    // 单个 ProduceRequest 允许的最大字节数，用于发送前校验消息大小。
    private final int maxRequestSize;
    // Producer 可用于缓存待发送消息的总内存上限，对应 buffer.memory。
    private final long totalMemorySize;
    // Producer 维护的集群元数据缓存，doSend 发送前必须确保 topic/partition 元数据可用。
    private final ProducerMetadata metadata;
    // 待发送消息的内存累加器，doSend 会把序列化后的消息追加到这里，由 Sender 线程异步发送。
    private final RecordAccumulator accumulator;
    // 后台网络发送器，负责从 accumulator 拉取批次并发送 ProduceRequest。
    private final Sender sender;
    // Sender 对应的后台 I/O 线程；doSend 在批次可发送时会唤醒该线程。
    private final Sender.SenderThread ioThread;
    // 当前 Producer 使用的压缩配置，用于估算消息大小和构建 RecordBatch。
    private final Compression compression;
    // 错误指标传感器，doSend 捕获异常时会记录。
    private final Sensor errors;
    // 时间抽象，便于统一处理等待元数据、buffer 分配和时间戳。
    private final Time time;
    // key 序列化器插件，doSend 会先把业务 key 转为字节数组。
    private final Plugin<Serializer<K>> keySerializerPlugin;
    // value 序列化器插件，doSend 会先把业务 value 转为字节数组。
    private final Plugin<Serializer<V>> valueSerializerPlugin;
    // Producer 配置对象，用于异常信息和发送流程中的配置读取。
    private final ProducerConfig producerConfig;
    // send 最多可阻塞时间，对元数据等待和 buffer 分配等阻塞步骤共同生效。
    private final long maxBlockTimeMs;
    // 是否忽略 key 参与分区；为 true 时即使 key 存在也可能走内置 sticky 分区逻辑。
    private final boolean partitionerIgnoreKeys;
    // Producer 拦截器链，send 前、ack 后、发送失败时都会被调用。
    private final ProducerInterceptors<K, V> interceptors;
    // Broker API 版本缓存，用于 Sender 判断请求能力和协议兼容性。
    private final ApiVersions apiVersions;
    // 事务管理器；启用事务或幂等时维护 producerId、sequence、事务分区和错误状态。
    private final TransactionManager transactionManager;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, new ApiVersions(), Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    // visible for testing
    @SuppressWarnings({"unchecked", "this-escape"})
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  ApiVersions apiVersions,
                  Time time) {
        try {
            this.producerConfig = config;
            this.time = time;

            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);

            this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);

            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            this.producerMetrics = new KafkaProducerMetrics(metrics);
            this.partitionerPlugin = Plugin.wrapInstance(
                    config.getConfiguredInstance(
                        ProducerConfig.PARTITIONER_CLASS_CONFIG,
                        Partitioner.class,
                        Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)),
                    metrics,
                    ProducerConfig.PARTITIONER_CLASS_CONFIG);
            this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            if (keySerializer == null) {
                keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
                keySerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            }
            this.keySerializerPlugin = Plugin.wrapInstance(keySerializer, metrics, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);

            if (valueSerializer == null) {
                valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
                valueSerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            }
            this.valueSerializerPlugin = Plugin.wrapInstance(valueSerializer, metrics, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);


            List<ProducerInterceptor<K, V>> interceptorList = (List<ProducerInterceptor<K, V>>) ClientUtils.configuredInterceptors(config,
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList, metrics);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    interceptorList,
                    reporters,
                    Arrays.asList(this.keySerializerPlugin.get(), this.valueSerializerPlugin.get()));
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compression = configureCompression(config);

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            this.apiVersions = apiVersions;
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        retryBackoffMaxMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses);
            }
            this.transactionManager = configureTransactionState(config, logContext);
            // There is no need to do work required for adaptive partitioning, if we use a custom partitioner.
            boolean enableAdaptivePartitioning = partitionerPlugin.get() == null &&
                config.getBoolean(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG);
            RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
                enableAdaptivePartitioning,
                config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG),
                config.getBoolean(ProducerConfig.PARTITIONER_RACK_AWARE_CONFIG),
                config.getString(ProducerConfig.CLIENT_RACK_CONFIG)
            );
            // As per Kafka producer configuration documentation batch.size may be set to 0 to explicitly disable
            // batching which in practice actually means using a batch size of 1.
            int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            this.accumulator = new RecordAccumulator(logContext,
                    batchSize,
                    compression,
                    lingerMs(config),
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    deliveryTimeoutMs,
                    partitionerConfig,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, batchSize, metrics, time, PRODUCER_METRIC_GROUP_NAME));

            this.errors = this.metrics.sensor("errors");
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new Sender.SenderThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    KafkaProducer(ProducerConfig config,
                  LogContext logContext,
                  Metrics metrics,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  TransactionManager transactionManager,
                  Sender sender,
                  ProducerInterceptors<K, V> interceptors,
                  Partitioner partitioner,
                  Time time,
                  Sender.SenderThread ioThread,
                  Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        this.producerConfig = config;
        this.time = time;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.log = logContext.logger(KafkaProducer.class);
        this.metrics = metrics;
        this.producerMetrics = new KafkaProducerMetrics(metrics);
        this.partitionerPlugin = Plugin.wrapInstance(partitioner, metrics, ProducerConfig.PARTITIONER_CLASS_CONFIG);
        this.keySerializerPlugin = Plugin.wrapInstance(keySerializer, metrics, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.valueSerializerPlugin = Plugin.wrapInstance(valueSerializer, metrics, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        this.interceptors = interceptors;
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compression = configureCompression(config);
        this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
        this.apiVersions = new ApiVersions();
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.errors = this.metrics.sensor("errors");
        this.metadata = metadata;
        this.sender = sender;
        this.ioThread = ioThread;
        this.clientTelemetryReporter = clientTelemetryReporter;
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : ClientUtils.createNetworkClient(producerConfig,
                this.metrics,
                "producer",
                logContext,
                apiVersions,
                time,
                maxInflightRequests,
                metadata,
                throttleTimeSensor,
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

        short acks = Short.parseShort(producerConfig.getString(ProducerConfig.ACKS_CONFIG));
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                producerConfig.getInt(ProducerConfig.RETRIES_CONFIG),
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager);
    }

    private static Compression configureCompression(ProducerConfig config) {
        CompressionType type = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        switch (type) {
            case GZIP: {
                return Compression.gzip()
                        .level(config.getInt(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                        .build();
            }
            case LZ4: {
                return Compression.lz4()
                        .level(config.getInt(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                        .build();
            }
            case ZSTD: {
                return Compression.zstd()
                        .level(config.getInt(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                        .build();
            }
            default:
                return Compression.of(type).build();
        }
    }

    private static int lingerMs(ProducerConfig config) {
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = lingerMs(config);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    private TransactionManager configureTransactionState(ProducerConfig config,
                                                         LogContext logContext) {
        TransactionManager transactionManager = null;

        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            final String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            final boolean enable2PC = config.getBoolean(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG);
            final int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            final long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            
            transactionManager = new TransactionManager(
                logContext,
                transactionalId,
                transactionTimeoutMs,
                retryBackoffMs,
                apiVersions,
                metadata,
                enable2PC
            );

            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        } else {
            // ignore unretrieved configurations related to producer transaction
            config.ignore(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        }
        return transactionManager;
    }

    /**
     * Initialize the transactional state for this producer, similar to {@link #initTransactions()} but
     * with additional capabilities to keep a previously prepared transaction.
     *
     * Needs to be called before any other methods when the {@code transactional.id} is set in the configuration.
     *
     * When {@code keepPreparedTxn} is {@code false}, this behaves like the standard transactional
     * initialization where the method does the following:
     * <ol>
     * <li>Ensures any transactions initiated by previous instances of the producer with the same
     *      {@code transactional.id} are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.</li>
     * <li>Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.</li>
     * </ol>
     *
     * <p>
     * When {@code keepPreparedTxn} is set to {@code true}, the producer does <em>not</em> automatically abort existing
     * transactions. Instead, it enters a recovery mode allowing only finalization of those previously
     * prepared transactions.
     * This behavior is especially crucial for 2PC scenarios, where transactions should remain intact
     * until the external transaction manager decides whether to commit or abort.
     * <p>
     *
     * @param keepPreparedTxn true to retain any in-flight prepared transactions (necessary for 2PC
     *                        recovery), false to abort existing transactions and behave like
     *                        the standard initTransactions.
     *
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no {@code transactional.id} is configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not
     *         support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.TransactionalIdAuthorizationException if the configured
     *         {@code transactional.id} is unauthorized either for normal transaction writes or 2PC.
     * @throws KafkaException if the producer encounters a fatal error or any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions(boolean keepPreparedTxn) {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        throwIfInPreparedState();
        long now = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.initializeTransactions(keepPreparedTxn);
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS, INIT_TXN_TIMEOUT_MSG);
        producerMetrics.recordInit(time.nanoseconds() - now);
        transactionManager.maybeUpdateTransactionV2Enabled(true);
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no {@code transactional.id} has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         {@code transactional.id} is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        throwIfInPreparedState();
        long now = time.nanoseconds();
        transactionManager.beginTransaction();
        producerMetrics.recordBeginTxn(time.nanoseconds() - now);
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. {@code nextRecordToBeProcessed.offset()}
     * (or {@link ConsumerRecords#nextOffsets()}). You should also add the leader epoch as commit metadata,
     * which can be obtained from {@link ConsumerRecord#leaderEpoch()} or {@link ConsumerRecords#nextOffsets()}.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code groupMetadata} should be extracted from the used {@link KafkaConsumer consumer} via
     * {@link KafkaConsumer#groupMetadata()} to leverage consumer group metadata. This will provide
     * stronger fencing than just supplying the {@code consumerGroupId} and passing in {@code new ConsumerGroupMetadata(consumerGroupId)},
     * however note that the full set of consumer group metadata returned by {@link KafkaConsumer#groupMetadata()}
     * requires the brokers to be on version 2.5 or newer to understand.
     *
     * <p>
     * This method is a blocking call that waits until the request has been received and acknowledged by the consumer group
     * coordinator; but the offsets are not considered as committed until the transaction itself is successfully committed later (via
     * the {@link #commitTransaction()} call).
     *
     * <p>
     * Note, that the consumer should have {@code enable.auto.commit=false} and should
     * also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * This method will raise {@link TimeoutException} if the producer cannot resolve the metadata for the topics in
     * {@code offsets} and send the offsets before expiration of {@code max.block.ms}.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started.
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0) or
     *         the broker doesn't support the latest version of transactional API with all consumer group metadata
     *         (i.e. if its version is lower than 2.5.0).
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried
     *         (e.g. if the consumer has been kicked out of the group). Users should handle this by aborting the transaction.
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this producer instance gets fenced by broker due to a
     *                                                                  mis-configured consumer instance id within group metadata.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the combined time taken for resolving topic metadata and sending the offsets
     *         has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throwIfInvalidGroupMetadata(groupMetadata);
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        throwIfInPreparedState();

        if (!offsets.isEmpty()) {
            long start = time.nanoseconds();
            var topics = offsets.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet());
            var waitMs = awaitTopicMetadata(topics);
            var remainingMs = Math.max(0L, maxBlockTimeMs - waitMs);
            TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
            sender.wakeup();
            result.await(remainingMs, TimeUnit.MILLISECONDS, SEND_OFFSETS_TIMEOUT_MSG);
            producerMetrics.recordSendOffsets(time.nanoseconds() - start);
        }
    }

    /**
     * Request a partial metadata refresh for the given topics and await the next
     * metadata update on a best-effort basis (up to {@code max.block.ms}). Returns
     * the elapsed wait time so the caller can subtract it from its own
     * {@code max.block.ms} budget.
     */
    private long awaitTopicMetadata(Set<String> topics) {
        long startNanos = time.nanoseconds();
        OptionalInt versionOpt = metadata.add(topics, time.milliseconds());
        if (versionOpt.isEmpty()) return 0L;
        sender.wakeup();
        try {
            metadata.awaitUpdate(versionOpt.getAsInt(), maxBlockTimeMs);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
        long elapsedNanos = time.nanoseconds() - startNanos;
        producerMetrics.recordMetadataWait(elapsedNanos);
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
    }

    /**
     * Prepares the current transaction for a two-phase commit. This method will flush all pending messages
     * and transition the producer into a mode where only {@link #commitTransaction()}, {@link #abortTransaction()},
     * or completeTransaction(PreparedTxnState) may be called.
     * <p>
     * This method is used as part of a two-phase commit protocol:
     * <ol>
     *   <li>Prepare the transaction by calling this method. This returns a {@link PreparedTxnState} if successful.</li>
     *   <li>Make any external system changes that need to be atomic with this transaction.</li>
     *   <li>Complete the transaction by calling {@link #commitTransaction()}, {@link #abortTransaction()} or
     *       completeTransaction(PreparedTxnState).</li>
     * </ol>
     *
     * @return the prepared transaction state to use when completing the transaction
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started yet.
     * @throws InvalidTxnStateException if the producer is not in a state where preparing
     *         a transaction is possible or 2PC is not enabled.
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for preparing the transaction has surpassed <code>max.block.ms</code>
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Override
    public PreparedTxnState prepareTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        throwIfInPreparedState();
        if (!transactionManager.is2PCEnabled()) {
            throw new InvalidTxnStateException("Cannot prepare a transaction when 2PC is not enabled");
        }
        long now = time.nanoseconds();
        flush();
        transactionManager.prepareTransaction();
        producerMetrics.recordPrepareTxn(time.nanoseconds() - now);
        ProducerIdAndEpoch producerIdAndEpoch = transactionManager.preparedTransactionState();
        return new PreparedTxnState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     * <p>
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     * <p>
     * If the transaction is committed successfully and this method returns without throwing an exception, it is guaranteed
     * that all {@link Callback callbacks} for records in the transaction will have been invoked and completed.
     * Note that exceptions thrown by callbacks are ignored; the producer proceeds to commit the transaction in any case.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
     * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long commitStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS, COMMIT_TXN_TIMEOUT_MSG);
        producerMetrics.recordCommitTxn(time.nanoseconds() - commitStart);
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as {@link #commitTransaction})
     * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        log.info("Aborting incomplete transaction");
        long abortStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS, ABORT_TXN_TIMEOUT_MSG);
        producerMetrics.recordAbortTxn(time.nanoseconds() - abortStart);
    }

    /**
     * Completes a prepared transaction by comparing the provided prepared transaction state with the
     * current prepared state on the producer.
     * If they match, the transaction is committed; otherwise, it is aborted.
     * 
     * @param preparedTxnState              The prepared transaction state to compare against the current state
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws InvalidTxnStateException if the producer is not in prepared state
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for completing the transaction has surpassed <code>max.block.ms</code>
     * @throws InterruptException if the thread is interrupted while blocked
     */
    @Override
    public void completeTransaction(PreparedTxnState preparedTxnState) throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        
        if (!transactionManager.isPrepared()) {
            throw new InvalidTxnStateException("Cannot complete transaction because no transaction has been prepared. " +
                "Call prepareTransaction() first, or make sure initTransaction(true) was called.");
        }
        
        // Get the current prepared transaction state
        ProducerIdAndEpoch currentProducerIdAndEpoch = transactionManager.preparedTransactionState();
        PreparedTxnState currentPreparedState = new PreparedTxnState(currentProducerIdAndEpoch.producerId, currentProducerIdAndEpoch.epoch);
        
        // Compare the prepared transaction state token and commit or abort accordingly
        if (currentPreparedState.equals(preparedTxnState)) {
            commitTransaction();
        } else {
            abortTransaction();
        }
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately (except for rare cases described below)
     * once the record has been stored in the buffer of records waiting to be sent.
     * This allows sending many records in parallel without blocking to wait for the response after each one.
     * Can block for the following cases: 1) For the first record being sent to 
     * the cluster by this client for the given topic. In this case it will block for up to {@code max.block.ms} milliseconds 
     * while waiting for topic's metadata if Kafka cluster is unreachable; 2) Allocating a buffer if buffer pool doesn't
     * have any free buffers.
     * <p>
     * <b>Reducing first-send latency:</b> You can reduce the latency of the first send by preloading the metadata
     * with {@link #partitionsFor(String)}. However, be aware that metadata cache will be cleared to free up resources after
     * {@code metadata.max.idle.ms} of inactivity, so subsequent sends after a long idle period will still
     * experience delays.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If the producer is configured with acks = 0, the {@link RecordMetadata}
     * will have offset = -1 because the producer does not wait for the acknowledgement from the broker.
     * If {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future
     * in order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error,
     * the final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When
     * this happens, your application should call {@link #abortTransaction()} to reset the state and continue to send
     * data.
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}.  In particular,
     * if a transactional send finishes with a {@link ProducerFencedException}, a {@link org.apache.kafka.common.errors.OutOfOrderSequenceException},
     * a {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an
     * {@link org.apache.kafka.common.errors.AuthorizationException}, then the only option left is to call {@link #close()}.
     * Fatal errors cause the producer to enter a defunct state in which future API calls will continue to raise
     * the same underlying error wrapped in a new {@link KafkaException}.
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured.
     * In this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and
     * {@link org.apache.kafka.common.errors.AuthorizationException} are considered fatal errors. However,
     * {@link ProducerFencedException} does not need to be handled. Additionally, it is possible to continue
     * sending after receiving an {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so
     * can result in out of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional
     * produce requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}
     * error. If this is encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is upgraded.
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record   The record to send. If the topic or the partition specified in it cannot be found
     *                 in metadata within {@code max.block.ms}, the returned future will time out when retrieved.
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *                 indicates no callback)
     * @throws IllegalStateException  if a transactional.id has been configured and no transaction has been started, or
     *                                when send is invoked after producer has been closed.
     * @throws InterruptException     If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws KafkaException         If a Kafka related error occurs that does not belong to the public API exceptions.
     * @see #partitionsFor(String)
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        // 发送入口的第一步是执行 ProducerInterceptor#onSend 链。
        // 拦截器可能返回一个被修改过的 ProducerRecord，后续 doSend 使用的就是这个新 record。
        // 这里不会向外抛出拦截器异常，ProducerInterceptors 内部会捕获并继续执行后续拦截器。
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        // doSend 是真正进入序列化、分区、累加器写入和 Sender 唤醒的核心发送实现。
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    // 校验当前 Producer 实例是否仍处于可发送状态；如果 Producer 已关闭，则抛出 IllegalStateException。
    private void throwIfProducerClosed() {
        // Sender 不存在或已停止时，说明 Producer 生命周期已经结束，不能再接受新消息。
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Throws an exception if the transaction is in a prepared state.
     * In a two-phase commit (2PC) flow, once a transaction enters the prepared state,
     * only commit, abort, or complete operations are allowed.
     * 如果事务已经进入 prepared 状态，则禁止继续执行普通发送操作。
     * 在两阶段提交流程中，prepared 之后只能提交、回滚或完成事务，不能再追加新消息。
     *
     * @throws IllegalStateException if any other operation is attempted in the prepared state.
     */
    private void throwIfInPreparedState() {
        // 事务处于 prepared 阶段时已经进入两阶段提交的决议窗口。
        // 此时再发送新消息会破坏事务边界，因此只允许提交、回滚或完成事务。
        if (transactionManager != null &&
            transactionManager.isTransactional() &&
            transactionManager.isPrepared()
        ) {
            throw new IllegalStateException("Cannot perform operation while the transaction is in a prepared state. " +
                "Only commitTransaction(), abortTransaction(), or completeTransaction() are permitted.");
        }
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     * 异步发送一条消息到 Kafka Topic 的核心实现。
     *
     * 中文阅读提示：
     * 该方法仍然是异步发送，它只负责把消息完成发送前处理并放入 RecordAccumulator。
     * 真正的网络发送由后台 Sender 线程完成，返回的 Future 会在 broker 响应或发送失败后完成。
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        // Append callback takes care of the following:
        //  - call interceptors and user callback on completion
        //  - remember partition that is calculated in RecordAccumulator.append
        // AppendCallbacks 是发送链路中的回调适配器：
        //  - 发送完成时先调用拦截器的 acknowledgement，再调用用户 callback
        //  - 保存 RecordAccumulator.append 最终确定的分区，供事务登记、异常回调和日志使用
        AppendCallbacks appendCallbacks = new AppendCallbacks(callback, this.interceptors, record);

        try {
            // 阶段 1：生命周期和事务状态检查。任何发送前置条件不满足，都不能进入序列化和入队。
            throwIfProducerClosed();
            throwIfInPreparedState();

            // first make sure the metadata for the topic is available
            // 阶段 2：确保 Topic 元数据可用。
            // Producer 需要知道 topic 是否存在、分区数量是多少、指定分区是否合法。
            long nowMs = time.milliseconds();
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                // 如果本地 metadata 已满足发送条件，waitOnMetadata 会立即返回；
                // 否则会触发 metadata 更新并阻塞等待，最多等待 max.block.ms。
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            // 把等待 metadata 消耗的时间计入当前发送流程时间。
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            // 剩余可阻塞时间会继续传给 accumulator，用于可能发生的 buffer 分配等待。
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            // 当前可用的集群元数据快照，后续序列化、分区和 append 都基于这个视图。
            Cluster cluster = clusterAndWaitTime.cluster;

            // 阶段 3：序列化 key。Kafka 网络协议发送的是字节数组，业务对象必须先转成 byte[]。
            byte[] serializedKey;
            try {
                serializedKey = keySerializerPlugin.get().serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                // key 实际类型与 key.serializer 期望类型不匹配时，转换为更明确的 SerializationException。
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }

            // 阶段 4：序列化 value。value 为 null 时序列化器也需要按自身语义处理 tombstone/null value。
            byte[] serializedValue;
            try {
                serializedValue = valueSerializerPlugin.get().serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                // value 实际类型与 value.serializer 期望类型不匹配时，转换为更明确的 SerializationException。
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }

            // Try to calculate partition, but note that after this call it can be RecordMetadata.UNKNOWN_PARTITION,
            // which means that the RecordAccumulator would pick a partition using built-in logic (which may
            // take into account broker load, the amount of data produced to each partition, etc.).
            // 阶段 5：尝试提前计算分区。
            // 用户显式指定分区或配置自定义 Partitioner 时，通常会在这里得到确定分区。
            // 对无 key 或忽略 key 的消息，可能返回 UNKNOWN_PARTITION，交给 accumulator 的 sticky 逻辑选择。
            int partition = partition(record, serializedKey, serializedValue, cluster);

            // headers 已参与序列化和大小估算前置准备，后续不允许再被应用或拦截器修改。
            setReadOnly(record.headers());
            // 转成数组后传给 RecordBatch 构建逻辑，避免后续遍历可变 Headers 对象。
            Header[] headers = record.headers().toArray();

            // 阶段 6：估算单条记录的最大序列化体积。
            // 这里估算的是上界，因为真实压缩效果要到批次构建和压缩时才能确定。
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.CURRENT_MAGIC_VALUE,
                    compression.type(), serializedKey, serializedValue, headers);
            // 发送前快速失败，避免超大消息进入 accumulator 后再失败。
            ensureValidRecordSize(serializedSize);
            // 用户未指定 timestamp 时，使用 Producer 当前时间作为消息创建时间。
            long timestamp = record.timestamp() == null ? nowMs : record.timestamp();

            // Append the record to the accumulator.  Note, that the actual partition may be
            // calculated there and can be accessed via appendCallbacks.topicPartition.
            // 阶段 7：把消息追加到 RecordAccumulator。
            // RecordAccumulator 按 TopicPartition 维护 ProducerBatch 队列，Sender 后台线程会从这里取批次发送。
            // 如果前面分区未知，append 内部会通过内置 sticky/adaptive 分区逻辑确定最终分区。
            RecordAccumulator.RecordAppendResult result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, nowMs, cluster);
            // append 后必须已经确定真实分区，否则后续事务登记和回调元数据都无法定位 TopicPartition。
            assert appendCallbacks.getPartition() != RecordMetadata.UNKNOWN_PARTITION;

            // Add the partition to the transaction (if in progress) after it has been successfully
            // appended to the accumulator. We cannot do it before because the partition may be
            // unknown. Note that the `Sender` will refuse to dequeue
            // batches from the accumulator until they have been added to the transaction.
            // 阶段 8：事务 Producer 需要把消息所在分区加入当前事务。
            // 这一步必须在 append 后执行，因为 append 前真实分区可能还是 UNKNOWN_PARTITION。
            // Sender 会等待分区加入事务后才允许发送对应批次，以保证事务语义。
            if (transactionManager != null) {
                transactionManager.maybeAddPartition(appendCallbacks.topicPartition());
            }

            if (result.batchIsFull || result.newBatchCreated) {
                // 阶段 9：批次已满或新批次创建时，唤醒后台 Sender 线程尽快发送。
                // 如果不唤醒，也会被 Sender 的轮询周期或 linger.ms 推进，但延迟可能更高。
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), appendCallbacks.getPartition());
                this.sender.wakeup();
            }

            // send 的成功路径只表示消息已进入本地缓冲区；broker ack 会异步完成这个 Future。
            return result.future;

            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
            // 异常处理策略：
            //  - ApiException 通过失败 Future 返回，维持 send 的异步 API 语义
            //  - InterruptedException 包装成 Kafka 公共 InterruptException 抛出
            //  - KafkaException 和未知异常直接抛出
        } catch (ApiException e) {
            // ApiException 通常表示 Kafka API/协议层面的可归类异常。
            // 这类异常通过 FutureFailure 返回给调用方，同时立即触发用户 callback 和 interceptor 失败回调。
            log.debug("Exception occurred during message send:", e);
            if (callback != null) {
                TopicPartition tp = appendCallbacks.topicPartition();
                // 构造失败场景下的占位 RecordMetadata；offset、timestamp 等不可用字段使用无效值。
                RecordMetadata nullMetadata = new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
                // 发送尚未进入 broker 成功确认路径，因此这里同步通知用户 callback 失败。
                callback.onCompletion(nullMetadata, e);
            }
            this.errors.record();
            // 拦截器的失败通知最终会转成 onAcknowledgement(metadata, exception, headers) 调用。
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            if (transactionManager != null) {
                // 事务场景下，发送异常可能导致事务进入 fatal 或 abortable 错误状态。
                transactionManager.maybeTransitionToErrorState(e);
            }
            // 返回一个已经完成且 get() 必定抛 ExecutionException 的 Future。
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            // 当前线程在等待 metadata 或 buffer 时被中断，记录指标并通知拦截器后抛出 Kafka 风格异常。
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw new InterruptException(e);
        } catch (KafkaException e) {
            // KafkaException 已经是 Kafka 客户端可识别异常，记录指标并通知拦截器后保持原异常向外抛出。
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            // send 入口最先调用了 interceptor.onSend，所以这里对所有未预期异常也必须通知拦截器。
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        // ProducerRecord 的 headers 默认可变；一旦进入发送流程，序列化和批次构建依赖其稳定性。
        // 标记为只读可以防止消息进入 accumulator 后仍被调用方修改。
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * 等待指定 topic 的集群元数据可用，包括 topic 是否存在以及分区信息是否满足发送要求。
     * 如果本地缓存不可用，会触发 metadata 更新并等待，最长不超过 maxWaitMs。
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param nowMs The current time in ms
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        // 先读取当前本地缓存的集群元数据快照。
        Cluster cluster = metadata.fetch();

        // 如果上一次元数据响应已经标记 topic 非法，直接失败，避免继续等待无意义的元数据刷新。
        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        // add topic to metadata topic list if it is not there already and reset expiry
        // 把当前 topic 加入 ProducerMetadata 的关注集合，确保后续 metadata 请求会包含它。
        // 即使 topic 已存在，也会刷新它的过期时间，避免发送活跃 topic 被清理。
        metadata.add(topic, nowMs);

        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        // 如果缓存中已经有 topic 分区数，且用户指定的分区没有越界，就可以直接使用缓存元数据。
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long remainingWaitMs = maxWaitMs;
        long elapsed = 0;
        // Issue metadata requests until we have metadata for the topic and the requested partition,
        // or until maxWaitTimeMs is exceeded. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        // 缓存元数据不可用或指定分区超过当前已知分区数时，循环触发 metadata 更新。
        // 典型场景：topic 刚创建、分区刚扩容、或本地 metadata 太旧。
        long nowNanos = time.nanoseconds();
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
            }
            // 再次刷新 topic 过期时间，避免等待期间被 metadata 清理逻辑移除。
            metadata.add(topic, nowMs + elapsed);
            // 请求对该 topic 进行 metadata 更新，并拿到当前版本号。
            // awaitUpdate 会等待元数据版本推进到大于该版本。
            int version = metadata.requestUpdateForTopic(topic);
            // metadata 请求由 Sender 线程发送；这里唤醒 Sender 让请求尽快发出。
            sender.wakeup();
            try {
                // 阻塞等待 metadata 版本推进，最长不超过剩余 max.block.ms。
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                final String errorMessage = getErrorMessage(partitionsCount, topic, partition, maxWaitMs);
                if (metadata.getError(topic) != null) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            // 读取更新后的元数据快照，并计算本轮等待累计耗时。
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - nowMs;
            if (elapsed >= maxWaitMs) {
                final String errorMessage = getErrorMessage(partitionsCount, topic, partition, maxWaitMs);
                if (metadata.getError(topic) != null && metadata.getError(topic).exception() instanceof RetriableException) {
                    throw new TimeoutException(errorMessage, metadata.getError(topic).exception());
                }
                throw new TimeoutException(errorMessage);
            }
            // 如果元数据响应中包含当前 topic 的不可恢复错误，例如无权限或非法 topic，这里抛出。
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        // 记录 Producer 在 metadata 等待上消耗的时间，便于观察发送前阻塞。
        producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos);

        // 返回最终可用的集群元数据，以及本次等待 metadata 的毫秒耗时。
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    private String getErrorMessage(Integer partitionsCount, String topic, Integer partition, long maxWaitMs) {
        // topic 元数据完全不存在，与指定分区超过已知分区数是两类常见超时原因，错误信息分开描述。
        return partitionsCount == null ?
            String.format("Topic %s not present in metadata after %d ms.",
                topic, maxWaitMs) :
            String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.",
                partition, topic, partitionsCount, maxWaitMs);
    }
    /**
     * Validate that the record size isn't too large
     * 校验单条消息序列化后的大小不能超过 Producer 的请求大小和缓冲区大小限制。
     */
    private void ensureValidRecordSize(int size) {
        // max.request.size 限制单个请求大小；单条消息超过它时不可能被合法发送。
        if (size > maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than " + maxRequestSize + ", which is the value of the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        // buffer.memory 是整个 Producer 的发送缓冲总量；单条消息超过总缓冲也无法进入 accumulator。
        if (size > totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>
     * and callbacks passed to {@link #send(ProducerRecord,Callback)} have been called).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     * <p>
     * <b>Important:</b> This method must not be called from within the callback provided to
     * {@link #send(ProducerRecord, Callback)}. Invoking <code>flush()</code> in this context will result in a
     * {@link KafkaException} being thrown, as it will cause a deadlock.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws KafkaException If the method is invoked inside a {@link #send(ProducerRecord, Callback)} callback
     */
    @Override
    public void flush() {
        if (Thread.currentThread() == this.ioThread) {
            log.error("KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.");
            throw new KafkaException("KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.");
        }

        log.trace("Flushing accumulated records in producer.");

        long start = time.nanoseconds();
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        } finally {
            producerMetrics.recordFlush(time.nanoseconds() - start);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * <p/>
     * This will attempt to refresh metadata until it finds the topic in it, or the configured {@link ProducerConfig#MAX_BLOCK_MS_CONFIG} expires.
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException  if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException      if the thread is interrupted while blocked
     * @throws TimeoutException        if the topic cannot be found in metadata within {@code max.block.ms}
     * @throws KafkaException          for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, time.milliseconds(), maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     *
     * <p>The returned map is an unmodifiable live view of the metrics. Changes to the underlying
     * metrics will be reflected in the returned map.
     *
     * @return An unmodifiable live view of the map of metrics currently maintained by the producer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }


    /**
     * Add the provided application metric for subscription.
     * This metric will be added to this client's metrics
     * that are available for subscription and sent as
     * telemetry data to the broker.
     * The provided metric must map to an OTLP metric data point
     * type in the OpenTelemetry v1 metrics protobuf message types.
     * Specifically, the metric should be one of the following:
     * <ul>
     *  <li>
     *     `Sum`: Monotonic total count meter (Counter). Suitable for metrics like total number of X, e.g., total bytes sent.
     *  </li>
     *  <li>
     *     `Gauge`: Non-monotonic current value meter (UpDownCounter). Suitable for metrics like current value of Y, e.g., current queue count.
     *  </li>
     * </ul>
     * Metrics not matching these types are silently ignored.
     * Executing this method for a previously registered metric is a benign operation and results in updating that metrics entry.
     *
     * @param metric The application metric to register
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        }  else {
            log.debug("Skipping registration for metric {}. Existing producer metrics cannot be overwritten.", metric.metricName());
        }
    }

    /**
     * Remove the provided application metric for subscription.
     * This metric is removed from this client's metrics
     * and will not be available for subscription any longer.
     * Executing this method with a metric that has not been registered is a
     * benign operation and does not result in any action taken (no-op).
     *
     * @param metric The application metric to remove
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        } else {
            log.debug("Skipping unregistration for metric {}. Existing producer metrics cannot be removed.", metric.metricName());
        }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the producer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ProducerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for producer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        producer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     * @return The client's assigned instance id used for metrics collection.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ProducerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                final Timer closeTimer = time.timer(timeout);
                clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
                closeTimer.update();

                if (this.sender != null) {
                    this.sender.initiateClose();
                    closeTimer.update();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(closeTimer.remainingMs());
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    } finally {
                        closeTimer.update();
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(producerMetrics, "producer metrics wrapper", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializerPlugin, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializerPlugin, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitionerPlugin, "producer partitioner", firstException);
        clientTelemetryReporter.ifPresent(reporter -> Utils.closeQuietly(reporter, "producer telemetry reporter", firstException));
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        log.debug("Kafka producer has been closed");
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * if custom partitioner is specified, call it to compute partition
     * otherwise try to calculate partition based on key.
     * If there is no key or key should be ignored return
     * RecordMetadata.UNKNOWN_PARTITION to indicate any partition
     * can be used (the partition is then calculated by built-in
     * partitioning logic).
     * 计算当前 record 的目标分区。
     * 优先使用用户显式指定的分区，其次调用自定义分区器，再其次对 key 做默认 hash 分区；
     * 如果没有 key 或配置要求忽略 key，则返回 UNKNOWN_PARTITION，由 RecordAccumulator 的内置分区逻辑决定。
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        // 优先级 1：用户在 ProducerRecord 中显式指定 partition，直接使用，不再走分区器。
        if (record.partition() != null)
            return record.partition();

        // 优先级 2：用户配置了自定义 Partitioner，则把原始 key/value 和序列化后的 key/value 都交给它。
        if (partitionerPlugin.get() != null) {
            int customPartition = partitionerPlugin.get().partition(
                record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
            // 自定义分区器只能返回非负分区号；是否超过分区数量由后续发送链路进一步处理。
            if (customPartition < 0) {
                throw new IllegalArgumentException(String.format(
                    "The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.", customPartition));
            }
            return customPartition;
        }

        // 优先级 3：存在 key 且配置允许 key 影响分区时，使用 Kafka 默认 murmur2 hash 保证相同 key 到同一分区。
        if (serializedKey != null && !partitionerIgnoreKeys) {
            // hash the keyBytes to choose a partition
            return BuiltInPartitioner.partitionForKey(serializedKey, cluster.partitionsForTopic(record.topic()).size());
        } else {
            // 优先级 4：无 key 或忽略 key 时先返回 UNKNOWN_PARTITION。
            // 后续 RecordAccumulator 会使用内置 sticky/adaptive 分区逻辑选择真实分区。
            return RecordMetadata.UNKNOWN_PARTITION;
        }
    }

    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");
        } else if (groupMetadata.generationId() > 0
            && JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but the member.id is unknown");
        }
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
        // 等待完成后可用于发送决策的集群元数据快照。
        final Cluster cluster;
        // 本次为等待 topic/partition 元数据实际消耗的毫秒数。
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        // 保存失败原因，并按 Future#get 的标准包装为 ExecutionException。
        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            // 发送前已经失败，没有可取消的后台任务。
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            // 失败 Future 一旦被 get，立即抛出发送阶段捕获到的异常。
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            // 失败已确定，不需要等待 timeout。
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            // KafkaProducer 的发送 Future 不支持取消语义。
            return false;
        }

        @Override
        public boolean isDone() {
            // FutureFailure 创建时就已经完成。
            return true;
        }

    }

    /**
     * Callbacks that are called by the RecordAccumulator append functions:
     *  - user callback
     *  - interceptor callbacks
     *  - partition callback
     * RecordAccumulator 追加消息时使用的回调适配器：
     *  - 负责触发用户 callback
     *  - 负责触发 ProducerInterceptor acknowledgement
     *  - 负责接收并保存 append 阶段最终确定的分区
     */
    private class AppendCallbacks implements RecordAccumulator.AppendCallbacks {
        // 用户调用 send(record, callback) 时传入的回调，最终在拦截器 acknowledgement 后执行。
        private final Callback userCallback;
        // Producer 拦截器链，用于发送完成或失败后的 onAcknowledgement 通知。
        private final ProducerInterceptors<K, V> interceptors;
        // record 的 topic，单独保存是为了避免 batch 生命周期内一直持有完整 ProducerRecord。
        private final String topic;
        // 用户显式指定的分区；如果 append 前后都无法确定分区，用于构造 TopicPartition。
        private final Integer recordPartition;
        // trace 日志中使用的 record 字符串，只在 trace 开启时保存，避免不必要开销。
        private final String recordLogString;
        // append 过程中回填的最终分区；volatile 保证 Sender/回调线程可见。
        private volatile int partition = RecordMetadata.UNKNOWN_PARTITION;
        // 根据 topic 和最终分区延迟构造的 TopicPartition。
        private volatile TopicPartition topicPartition;
        // 发送记录的 headers，ack/fail 回调给拦截器时需要传入。
        private final Headers headers;

        private AppendCallbacks(Callback userCallback, ProducerInterceptors<K, V> interceptors, ProducerRecord<K, V> record) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            // Extract record info as we don't want to keep a reference to the record during
            // whole lifetime of the batch.
            // We don't want to have an NPE here, because the interceptors would not be notified (see .doSend).
            // 只提取回调所需的轻量信息，不长期持有完整 record，避免 batch 生命周期拉长对象引用。
            // record 可能为 null，因此这里要防御空值，否则异常会导致拦截器收不到失败通知。
            topic = record != null ? record.topic() : null;
            if (record != null) {
                headers = record.headers();
            } else {
                // 没有 record 时仍提供只读空 headers，保证拦截器回调参数稳定。
                headers = new RecordHeaders();
                ((RecordHeaders) headers).setReadOnly();
            }
            recordPartition = record != null ? record.partition() : null;
            recordLogString = log.isTraceEnabled() && record != null ? record.toString() : "";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            // Sender 完成批次后会回调这里；metadata 为 null 时构造一个失败占位元数据。
            if (metadata == null) {
                metadata = new RecordMetadata(topicPartition(), -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
            }
            // Kafka 的回调顺序是先通知拦截器，再通知用户 callback。
            this.interceptors.onAcknowledgement(metadata, exception, headers);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }

        @Override
        public void setPartition(int partition) {
            // RecordAccumulator 在确定真实分区后调用该方法，把分区回填给 doSend 侧。
            assert partition != RecordMetadata.UNKNOWN_PARTITION;
            this.partition = partition;

            if (log.isTraceEnabled()) {
                // Log the message here, because we don't know the partition before that.
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", recordLogString, userCallback, topic, partition);
            }
        }

        public int getPartition() {
            // 返回 append 阶段回填的真实分区。
            return partition;
        }

        public TopicPartition topicPartition() {
            // 延迟构造 TopicPartition，优先使用 append 后确定的真实分区。
            if (topicPartition == null && topic != null) {
                if (partition != RecordMetadata.UNKNOWN_PARTITION)
                    topicPartition = new TopicPartition(topic, partition);
                else if (recordPartition != null)
                    // 如果 append 尚未回填分区，但用户原始 record 指定了分区，则使用用户指定值。
                    topicPartition = new TopicPartition(topic, recordPartition);
                else
                    // 仍未知时使用 UNKNOWN_PARTITION，主要用于异常回调中的占位元数据。
                    topicPartition = new TopicPartition(topic, RecordMetadata.UNKNOWN_PARTITION);
            }
            return topicPartition;
        }
    }
}
