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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.ShareSessionHandler;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.ShareFetchBuffer;
import org.apache.kafka.clients.consumer.internals.ShareFetchCollector;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.GroupMetadataUpdateEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscriptionChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;

/**
 * A client that consumes records from a Kafka cluster using a share group.
 */
public class KafkaShareConsumerNew<K, V> implements ShareConsumer<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;

    private class BackgroundEventProcessor extends EventProcessor<BackgroundEvent> {

        private final ApplicationEventHandler applicationEventHandler;
        public BackgroundEventProcessor(final LogContext logContext,
                                        final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                        final ApplicationEventHandler applicationEventHandler) {
            super(logContext, backgroundEventQueue);
            this.applicationEventHandler = applicationEventHandler;
        }

        /**
         * Process the events—if any—that were produced by the {@link ConsumerNetworkThread network thread}.
         * It is possible that {@link org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent an error}
         * could occur when processing the events. In such cases, the processor will take a reference to the first
         * error, continue to process the remaining events, and then throw the first error that occurred.
         */
        @Override
        public boolean process() {
            AtomicReference<KafkaException> firstError = new AtomicReference<>();

            ProcessHandler<BackgroundEvent> processHandler = (event, error) -> {
                if (error.isPresent()) {
                    KafkaException e = error.get();

                    if (!firstError.compareAndSet(null, e)) {
                        log.warn("An error occurred when processing the event: {}", e.getMessage(), e);
                    }
                }
            };

            boolean hadEvents = process(processHandler);

            if (firstError.get() != null)
                throw firstError.get();

            return hadEvents;
        }

        @Override
        public void process(final BackgroundEvent event) {
            switch (event.type()) {
                case ERROR:
                    process((ErrorBackgroundEvent) event);
                    break;

                case GROUP_METADATA_UPDATE:
                    process((GroupMetadataUpdateEvent) event);
                    break;

                default:
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

            }
        }

        private void process(final ErrorBackgroundEvent event) {
            throw event.error();
        }

        private void process(final GroupMetadataUpdateEvent event) {
            final ConsumerGroupMetadata currentGroupMetadata = KafkaShareConsumerNew.this.groupMetadata;
            KafkaShareConsumerNew.this.groupMetadata = new ConsumerGroupMetadata(
                    currentGroupMetadata.groupId(),
                    event.memberEpoch(),
                    event.memberId(),
                    currentGroupMetadata.groupInstanceId()
            );
        }
    }

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private ConsumerGroupMetadata groupMetadata;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private LogContext logContext;
    private Logger log;
    private final String clientId;
    private final String groupId;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final Deserializers<K, V> deserializers;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;

    /**
     * A thread-safe {@link ShareFetchBuffer fetch buffer} for the results that are populated in the
     * {@link ConsumerNetworkThread network thread} when the results are available. Because of the interaction
     * of the fetch buffer in the application thread and the network I/O thread, this is shared between the
     * two threads and is thus designed to be thread-safe.
     */
    private final ShareFetchBuffer fetchBuffer;
    private final ShareFetchCollector<K, V> fetchCollector;
    private Acknowledgements acknowledgements;

    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long retryBackoffMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing the AsyncKafkaConsumer
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refCount = new AtomicInteger(0);

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public KafkaShareConsumerNew(Map<String, Object> configs)  {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaShareConsumerNew(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaShareConsumerNew(Properties properties,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(Utils.propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaShareConsumerNew(Map<String, Object> configs,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToShareConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer,
                valueDeserializer,
                Time.SYSTEM,
                ApplicationEventHandler::new,
                ConsumerMetadata::new,
                new LinkedBlockingQueue<>());
    }

    @SuppressWarnings("unchecked")
    KafkaShareConsumerNew(final ConsumerConfig config,
                          final Deserializer<K> keyDeserializer,
                          final Deserializer<V> valueDeserializer,
                          final Time time,
                          final ApplicationEventHandlerFactory applicationEventHandlerFactory,
                          final ConsumerMetadataFactory metadataFactory,
                          final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                    config,
                    GroupRebalanceConfig.ProtocolType.SHARE
            );
            this.groupId = groupRebalanceConfig.groupId;
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            this.logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());

            log.debug("Initializing the Kafka share consumer");
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = time;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.metrics = createMetrics(config, time, reporters);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.sessionHandlers = new HashMap<>();

            this.subscriptions = createSubscriptionState(config, logContext);
            this.acknowledgements = Acknowledgements.empty();
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                    Collections.emptyList(),
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = metadataFactory.build(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            FetchConfig fetchConfig = new FetchConfig(config);

            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
                    logContext,
                    backgroundEventQueue
            );

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new ShareFetchBuffer(logContext);
            this.fetchCollector = new ShareFetchCollector<K, V>(logContext, fetchConfig, deserializers);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager,
                    null
            );
            Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplierForShareGroup(
                    time,
                    logContext,
                    null,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier,
                    Optional.empty()
            );
            Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                    logContext,
                    metadata,
                    applicationEventQueue,
                    requestManagersSupplier
            );
            this.applicationEventHandler = applicationEventHandlerFactory.build(
                    logContext,
                    time,
                    applicationEventQueue,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier
            );
            this.backgroundEventProcessor = new KafkaShareConsumerNew.BackgroundEventProcessor(
                    logContext,
                    backgroundEventQueue,
                    applicationEventHandler
            );
            this.groupMetadata = new ConsumerGroupMetadata(groupId,
                    JoinGroupRequest.UNKNOWN_GENERATION_ID,
                    JoinGroupRequest.UNKNOWN_MEMBER_ID,
                    Optional.empty());

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka share consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // auxiliary interface for testing
    interface ApplicationEventHandlerFactory {

        ApplicationEventHandler build(
                final LogContext logContext,
                final Time time,
                final BlockingQueue<ApplicationEvent> applicationEventQueue,
                final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                final Supplier<RequestManagers> requestManagersSupplier
        );

    }

    // auxiliary interface for testing
    interface ConsumerMetadataFactory {

        ConsumerMetadata build(
                final ConsumerConfig config,
                final SubscriptionState subscriptions,
                final LogContext logContext,
                final ClusterResourceListeners clusterResourceListeners
        );

    }

    /**
     * Acknowledge successful delivery of a record returned on the last {@link #poll(Duration) poll()}. The
     * acknowledgement is committed on the next {@link #commitSync()} call.
     *
     * @param record The record whose delivery is being acknowledged
     */
    @Override
    public void acknowledge(ConsumerRecord record) {
        acknowledge(record, AcknowledgeType.ACCEPT);
    }

    /**
     * Acknowledge delivery of a record returned on the last {@link #poll(Duration) poll()} indicating whether
     * it was processed successfully. The acknowledgement is committed on the next {@link #commitSync()} call.
     *
     * @param record The record whose delivery is being acknowledged
     * @param type The acknowledgement type
     */
    @Override
    public void acknowledge(ConsumerRecord record, AcknowledgeType type) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            Map<String, Uuid> topicIds = metadata.topicIds();
            TopicIdPartition topicIdPartition =
                    new TopicIdPartition(topicIds.getOrDefault(record.topic(), Uuid.ZERO_UUID),
                            record.partition(),
                            record.topic());
            acknowledgements.add(topicIdPartition, record.offset(), type);
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection)}, or an empty set if no such call has been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.subscription());
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b>
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (topics == null) {
                throw new IllegalArgumentException("Topic collection to subscribe cannot be null");
            }
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                unsubscribe();
            } else {
                for (String topic : topics) {
                    if (isBlank(topic)) {
                        throw new IllegalArgumentException("Topic collection to subscribe cannot contain null or empty topics");
                    }
                }

                log.info("Subscribed to topic(s): {}", join(topics, ", "));
                if (subscriptions.subscribe(new HashSet<>(topics), Optional.empty()))
                    metadata.requestUpdateForNewTopics();

                // Trigger subscribe event to effectively join the group if not already part of it,
                // or just send the new subscription to the broker.
                applicationEventHandler.add(new SubscriptionChangeApplicationEvent());
            }
        } finally {
            release();
        }
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}.
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    @Override
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetchBuffer.retainAll(Collections.emptySet());

            UnsubscribeApplicationEvent unsubscribeApplicationEvent = new UnsubscribeApplicationEvent();
            Timer timer = time.timer(Long.MAX_VALUE);
            applicationEventHandler.addAndGet(unsubscribeApplicationEvent, timer);
            log.info("Unsubscribing all topics or patterns and assigned partitions");

            subscriptions.unsubscribe();
        } finally {
            release();
        }
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     *
     * <p>
     * This method returns immediately if there are records available or if the position advances past control records
     * or aborted transactions when isolation.level=read_committed.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
     **
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions,
     *             or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws java.lang.ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        Timer timer = time.timer(timeout);

        acquireAndEnsureOpen();
        try {
            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Share consumer is not subscribed to any topics");
            }

            do {
                backgroundEventProcessor.process();
                Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);
                if (!fetch.isEmpty()) {

                } else {
                    Timer pollTimer = time.timer(timer.remainingMs());
                    try {
                        fetchBuffer.awaitNotEmpty(pollTimer);
                    } finally {
                        timer.update(pollTimer.currentTimeMs());
                    }

                    fetch = fetchCollector.collectFetch(fetchBuffer);
                    if (!fetch.isEmpty()) {
                        return new ConsumerRecords<>(fetch.records());
                    }
                }
            } while(timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            release();
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This fatal error can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout specified by {@code default.api.timeout.ms} expires
     *            before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(1000000));//defaultApiTimeoutMs));
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Map<Node, ShareSessionHandler.AcknowledgeBuilder> fetchable = new LinkedHashMap<>();

            for (TopicIdPartition partition : acknowledgements.partitions()) {
                SubscriptionState.FetchPosition position = subscriptions.position(partition.topicPartition());

                if (position == null)
                    throw new IllegalStateException("Missing position for fetchable partition " + partition);

                Optional<Node> leaderOpt = position.currentLeader.leader;

                if (!leaderOpt.isPresent()) {
                    metadata.requestUpdate(false);
                    continue;
                }

                Node node = leaderOpt.get();

                // Build an AcknowledgementBatch for the topic-partition
                Map<Long, AcknowledgeType> ackMap = acknowledgements.forPartition(partition);
                if (ackMap != null) {
                    // if there is a leader and no in-flight requests, issue a new request
                    ShareSessionHandler.AcknowledgeBuilder builder = fetchable.computeIfAbsent(node, k -> {
                        ShareSessionHandler shareSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n));
                        return shareSessionHandler.newAcknowledgeBuilder();
                    });

                    ShareAcknowledgeRequest.AcknowledgementBatch ackBatch = null;
                    for (Map.Entry<Long, AcknowledgeType> e: ackMap.entrySet()) {
                        if (ackBatch == null) {
                            ackBatch = new ShareAcknowledgeRequest.AcknowledgementBatch(e.getKey(), e.getKey(), e.getValue());
                        } else {
                            if ((e.getKey() == ackBatch.lastOffset + 1) && (e.getValue() == ackBatch.type)) {
                                ackBatch = new ShareAcknowledgeRequest.AcknowledgementBatch(ackBatch.startOffset, e.getKey(), ackBatch.type);
                            } else {
                                builder.add(partition, ackBatch);
                                ackBatch = new ShareAcknowledgeRequest.AcknowledgementBatch(e.getKey(), e.getKey(), e.getValue());
                            }
                        }
                    }
                    if (ackBatch != null) {
                        builder.add(partition, ackBatch);
                    }

                    acknowledgements.clearForPartition(partition);

                    log.debug("Added acknowledge request for partition {} to node {}", partition, node);

                    final ShareAcknowledgeApplicationEvent ackEvent = new ShareAcknowledgeApplicationEvent(
                            node,
                            builder.build()
                    );
                    applicationEventHandler.add(ackEvent);
                    ConsumerUtils.getResult(ackEvent.future(), time.timer(timeout));
                }
            }
        } finally {
            release();
        }
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     *             before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {

    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka share consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        final Timer closeTimer = time.timer(timeout);
        // Prepare shutting down the network thread
        prepareShutdown(closeTimer, firstException);
        closeTimer.update();
        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        closeTimer.update();
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);

        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka share consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close Kafka share consumer", exception);
        }
    }

    /**
     * Prior to closing the network thread, we need to make sure the following operations happen in the right sequence:
     * 1. autocommit offsets
     * 2. revoke all partitions
     * 3. if partition revocation completes successfully, send leave group
     * 4. invoke all async commit callbacks if there is any
     */
    void prepareShutdown(final Timer timer, final AtomicReference<Throwable> firstException) {
//        if (!groupMetadata.isPresent())
//            return;
//        maybeAutoCommitSync(autoCommitEnabled, timer, firstException);
//        applicationEventHandler.add(new CommitOnCloseApplicationEvent());
//        completeQuietly(
//                () -> {
//                    maybeRevokePartitions();
//                    applicationEventHandler.addAndGet(new LeaveOnCloseApplicationEvent(), timer);
//                },
//                "Failed to send leaveGroup heartbeat with a timeout(ms)=" + timer.timeoutMs(), firstException);
//        swallow(log, Level.ERROR, "Failed invoking asynchronous commit callback.", this::maybeInvokeCommitCallbacks,
//                firstException);
    }

    // Visible for testing
    void completeQuietly(final Utils.ThrowingRunnable function,
                         final String msg,
                         final AtomicReference<Throwable> firstException) {
        try {
            function.run();
        } catch (TimeoutException e) {
            log.debug("Timeout expired before the {} operation could complete.", msg);
        } catch (Exception e) {
            firstException.compareAndSet(null, e);
        }
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded usage is not
     * supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        refCount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multithreaded access.
     */
    private void release() {
        if (refCount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void maybeThrowInvalidGroupIdException() {
        if (groupId.isEmpty())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }
}