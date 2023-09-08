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
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.FetchMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.ShareFetcher;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A client that consumes records from a Kafka cluster using a share group.
 */
public class KafkaShareConsumer<K, V> {

  private static final String CLIENT_ID_METRIC_TAG = "client-id";
  private static final long NO_CURRENT_THREAD = -1L;
  private static final String JMX_PREFIX = "kafka.consumer";
  static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

  // Visible for testing
  final Metrics metrics;
  final KafkaConsumerMetrics kafkaConsumerMetrics;

  private Logger log;
  private final String clientId;
  private final String groupId;
  private final ConsumerCoordinator coordinator;
  private final Deserializers<K, V> deserializers;
  private final ShareFetcher<K, V> fetcher;
  private Acknowledgements acknowledgements;
  private final ConsumerInterceptors<K, V> interceptors;

  private final Time time;
  private final ConsumerNetworkClient client;
  private final SubscriptionState subscriptions;
  private final ConsumerMetadata metadata;
  private final long retryBackoffMs;
  private final long requestTimeoutMs;
  private volatile boolean closed = false;
  private final List<ConsumerPartitionAssignor> assignors;

  // currentThread holds the threadId of the current thread accessing KafkaConsumer
  // and is used to prevent multi-threaded access
  private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
  // refcount is used to allow reentrant access by the thread who has acquired currentThread
  private final AtomicInteger refcount = new AtomicInteger(0);

  // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
  private boolean cachedSubscriptionHasAllFetchPositions;

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
  public KafkaShareConsumer(Map<String, Object> configs) {
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
  public KafkaShareConsumer(Properties properties) {
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
  public KafkaShareConsumer(Properties properties,
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
  public KafkaShareConsumer(Map<String, Object> configs,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer) {
    this(new ConsumerConfig(ConsumerConfig.appendDeserializerToShareConfig(configs, keyDeserializer, valueDeserializer)),
            keyDeserializer, valueDeserializer);
  }

  @SuppressWarnings("unchecked")
  KafkaShareConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    try {
      GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
              GroupRebalanceConfig.ProtocolType.SHARE);

      this.groupId = groupRebalanceConfig.groupId;
      this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);

      LogContext logContext;

      // If group.instance.id is set, we will append it to the log context.
      if (groupRebalanceConfig.groupInstanceId.isPresent()) {
        logContext = new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                ", clientId=" + clientId + ", groupId=" + groupId + "] ");
      } else {
        logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId + "] ");
      }

      this.log = logContext.logger(getClass());

      log.debug("Initializing the Kafka share consumer");
      this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
      this.time = Time.SYSTEM;
      this.metrics = buildMetrics(config, time, clientId);
      this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

      List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
              ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
              ConsumerInterceptor.class,
              Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
      this.interceptors = new ConsumerInterceptors<>(interceptorList);
      this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
      this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.NONE);
      this.acknowledgements = Acknowledgements.empty();
      ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
              metrics.reporters(),
              interceptorList,
              Arrays.asList(this.deserializers.keyDeserializer, this.deserializers.valueDeserializer));
      this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
      List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
              config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
      this.metadata.bootstrap(addresses);
      String metricGrpPrefix = "consumer";

      FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);
      FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(metrics, metricsRegistry);
      ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
      int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

      ApiVersions apiVersions = new ApiVersions();
      NetworkClient netClient = new NetworkClient(
              new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
              this.metadata,
              clientId,
              100, // a fixed large enough value will suffice for max in-flight requests
              config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
              config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
              config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
              config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
              config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
              config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
              config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
              time,
              true,
              apiVersions,
              fetchMetricsManager.throttleTimeSensor(),
              logContext);
      this.client = new ConsumerNetworkClient(
              logContext,
              netClient,
              metadata,
              time,
              retryBackoffMs,
              config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
              heartbeatIntervalMs); //Will avoid blocking an extended period of time to prevent heartbeat thread starvation

      this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
              config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
              config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
      );

      this.coordinator = new ConsumerCoordinator(groupRebalanceConfig,
              logContext,
              this.client,
              assignors,
              this.metadata,
              this.subscriptions,
              metrics,
              metricGrpPrefix,
              this.time,
              false, // enableAutoCommit,
              config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
              this.interceptors,
              config.getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED),
              config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
              Optional.empty());

      FetchConfig fetchConfig = new FetchConfig(config);
      this.fetcher = new ShareFetcher<>(
              logContext,
              this.client,
              this.groupId,
              this.metadata,
              this.subscriptions,
              fetchConfig,
              this.deserializers,
              fetchMetricsManager,
              this.time);
      this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, metricGrpPrefix);

      config.logUnused();
      AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
      log.debug("Kafka share consumer initialized");
    } catch (Throwable t) {
      // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
      // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
      if (this.log != null) {
        close(Duration.ZERO, true);
      }
      // now propagate the exception
      throw new KafkaException("Failed to construct kafka share consumer", t);
    }
  }

  // visible for testing
  KafkaShareConsumer(LogContext logContext,
                String clientId,
                ConsumerCoordinator coordinator,
                Deserializer<K> keyDeserializer,
                Deserializer<V> valueDeserializer,
                ShareFetcher<K, V> fetcher,
                ConsumerInterceptors<K, V> interceptors,
                Time time,
                ConsumerNetworkClient client,
                Metrics metrics,
                SubscriptionState subscriptions,
                ConsumerMetadata metadata,
                long retryBackoffMs,
                long requestTimeoutMs,
                int defaultApiTimeoutMs,
                List<ConsumerPartitionAssignor> assignors,
                String groupId) {
    this.log = logContext.logger(getClass());
    this.clientId = clientId;
    this.coordinator = coordinator;
    this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
    this.fetcher = fetcher;
    this.acknowledgements = Acknowledgements.empty();
    this.interceptors = Objects.requireNonNull(interceptors);
    this.time = time;
    this.client = client;
    this.metrics = metrics;
    this.subscriptions = subscriptions;
    this.metadata = metadata;
    this.retryBackoffMs = retryBackoffMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.assignors = assignors;
    this.groupId = groupId;
    this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
  }

  private static Metrics buildMetrics(ConsumerConfig config, Time time, String clientId) {
    Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
    MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            .tags(metricsTags);
    List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
    MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
    return new Metrics(metricConfig, reporters, time, metricsContext);
  }

  /**
   * Get the current subscription. Will return the same topics used in the most recent call to
   * {@link #subscribe(Collection)}, or an empty set if no such call has been made.
   * @return The set of topics currently subscribed to
   */
  public Set<String> subscription() {
    acquireAndEnsureOpen();
    try {
      return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
    } finally {
      release();
    }
  }

  /**
   * Subscribe to the given list of topics to get dynamically
   * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
   * assignment (if there is one).
   *
   * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
   *
   * @param topics The list of topics to subscribe to
   * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
   * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
   *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
   *                               configured at-least one partition assignment strategy
   */
  public void subscribe(Collection<String> topics) {
    acquireAndEnsureOpen();
    try {
      maybeThrowInvalidGroupIdException();
      if (topics == null)
        throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
      if (topics.isEmpty()) {
        // treat subscribing to empty topic list as the same as unsubscribing
        this.unsubscribe();
      } else {
        for (String topic : topics) {
          if (Utils.isBlank(topic))
            throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
        }

        throwIfNoAssignorsConfigured();
        fetcher.clearBufferedDataForUnassignedTopics(topics);
        log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
        if (this.subscriptions.subscribe(new HashSet<>(topics), Optional.empty()))
          metadata.requestUpdateForNewTopics();
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
  public void unsubscribe() {
    acquireAndEnsureOpen();
    try {
      fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
      if (this.coordinator != null) {
        this.coordinator.onLeavePrepare();
        this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
      }
      this.subscriptions.unsubscribe();
      log.info("Unsubscribed all topics or patterns and assigned partitions");
    } finally {
      release();
    }
  }

  /**
   * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
   * subscribed to any topics or partitions before polling for data.
   *
   * <p>
   * This method returns immediately if there are records available or if the position advances past control records
   * or aborted transactions when isolation.level=read_committed.
   * Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
   * Note that this method may block beyond the timeout in order to execute custom
   * {@link ConsumerRebalanceListener} callbacks.
   *
   *
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
  public ConsumerRecords<K, V> poll(final Duration timeout) {
    return poll(time.timer(timeout), true);
  }

  /**
   * @throws KafkaException if the rebalance callback throws exception
   */
  private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
    acquireAndEnsureOpen();
    try {
      this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

      if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
        throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
      }

      do {
        client.maybeTriggerWakeup();

        if (includeMetadataInTimeout) {
          // try to update assignment metadata BUT do not need to block on the timer for join group
          updateAssignmentMetadataIfNeeded(timer, false);
        } else {
          while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
            log.warn("Still waiting for metadata");
          }
        }

        final Fetch<K, V> fetch = pollForFetches(timer);
        if (!fetch.isEmpty()) {
          // before returning the fetched records, we can send off the next round of fetches
          // and avoid block waiting for their responses to enable pipelining while the user
          // is handling the fetched records.
          //
          // NOTE: since the consumed position has already been updated, we must not allow
          // wakeups or any other errors to be triggered prior to returning the fetched records.
          if (sendFetches() > 0 || client.hasPendingRequests()) {
            client.transmitSends();
          }

          if (fetch.records().isEmpty()) {
            log.trace("Returning empty records from `poll()` "
                    + "since the consumer's position has advanced for at least one topic partition");
          }

          return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
        }
      } while (timer.notExpired());

      return ConsumerRecords.empty();
    } finally {
      release();
      this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
    }
  }

  private int sendFetches() {
//    offsetFetcher.validatePositionsOnMetadataChange();
    return fetcher.sendFetches();
  }

  boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
    if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
      return false;
    }

    return true;//updateFetchPositions(timer);
  }

  /**
   * @throws KafkaException if the rebalance callback throws exception
   */
  private Fetch<K, V> pollForFetches(Timer timer) {
    long pollTimeout = coordinator == null ? timer.remainingMs() :
            Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

    // if data is available already, return it immediately
    final Fetch<K, V> fetch = fetcher.collectFetch();
    if (!fetch.isEmpty()) {
      return fetch;
    }

    // send any new fetches (won't resend pending fetches)
    sendFetches();

    // We do not want to be stuck blocking in poll if we are missing some positions
    // since the offset lookup may be backing off after a failure

    // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
    // updateAssignmentMetadataIfNeeded before this method.
    if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
      pollTimeout = retryBackoffMs;
    }

    log.trace("Polling for fetches with timeout {}", pollTimeout);

    Timer pollTimer = time.timer(pollTimeout);
    client.poll(pollTimer, () -> {
      // since a fetch might be completed by the background thread, we need this poll condition
      // to ensure that we do not block unnecessarily in poll()
      return !fetcher.hasAvailableFetches();
    });
    timer.update(pollTimer.currentTimeMs());

    return fetcher.collectFetch();
  }

  /**
   * Acknowledge successful delivery of a record returned on the last {@link #poll(Duration) poll()}. The
   * acknowledgement is committed on the next {@link #commitSync()} or {@link #commitAsync()} call.
   *
   * @param record The record whose delivery is being acknowledged
   */
  public void acknowledge(ConsumerRecord record) {
    acknowledge(record, AcknowledgeType.ACCEPT);
  }

  /**
   * Acknowledge delivery of a record returned on the last {@link #poll(Duration) poll()} indicating whether
   * it was processed successfully. The acknowledgement is committed on the next {@link #commitSync()} or
   * {@link #commitAsync()} call.
   *
   * @param record The record whose delivery is being acknowledged
   * @param type The acknowledgement type
   */
  public void acknowledge(ConsumerRecord record, AcknowledgeType type) {
    System.out.println("**** AJS : KafkaShareConsumer.acknowledge(" + record.offset() + "," + type.toString() + ") ****");
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
   * Commit offsets returned on the last {@link #poll(Duration)} for all the subscribed list of topics and partition.
   * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
   */
  public void commitAsync() {
    acquireAndEnsureOpen();
    try {
      maybeThrowInvalidGroupIdException();
      log.debug("Committing acknowledgements: {}", acknowledgements);
      System.out.println("**** AJS : KafkaShareConsumer.commitAsync(" + acknowledgements + ") ****");
      fetcher.acknowledge(acknowledgements);
      acknowledgements = Acknowledgements.empty();
    } finally {
      release();
    }
  }

  /**
   * Get the metrics kept by the consumer
   */
  public Map<MetricName, ? extends Metric> metrics() {
    return Collections.unmodifiableMap(this.metrics.metrics());
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
  public void wakeup() {
    this.client.wakeup();
  }

  private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
    ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
    for (List<?> candidateList: candidateLists)
      clusterResourceListeners.maybeAddAll(candidateList);

    clusterResourceListeners.maybeAdd(keyDeserializer);
    clusterResourceListeners.maybeAdd(valueDeserializer);
    return clusterResourceListeners;
  }

  private Timer createTimerForRequest(final Duration timeout) {
    // this.time could be null if an exception occurs in constructor prior to setting the this.time field
    final Time localTime = (time == null) ? Time.SYSTEM : time;
    return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
  }

  private void close(Duration timeout, boolean swallowException) {
    log.trace("Closing the Kafka share consumer");
    AtomicReference<Throwable> firstException = new AtomicReference<>();

    final Timer closeTimer = createTimerForRequest(timeout);
    // Close objects with a timeout. The timeout is required because the coordinator & the fetcher send requests to
    // the server in the process of closing which may not respect the overall timeout defined for closing the
    // consumer.
    if (coordinator != null) {
      // This is a blocking call bound by the time remaining in closeTimer
      Utils.swallow(log, Level.ERROR, "Failed to close coordinator with a timeout(ms)=" + closeTimer.timeoutMs(), () -> coordinator.close(closeTimer), firstException);
    }

    if (fetcher != null) {
      // the timeout for the session close is at-most the requestTimeoutMs
      long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
      if (remainingDurationInTimeout > 0) {
        remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
      }

      closeTimer.reset(remainingDurationInTimeout);

      // This is a blocking call bound by the time remaining in closeTimer
      Utils.swallow(log, Level.ERROR, "Failed to close fetcher with a timeout(ms)=" + closeTimer.timeoutMs(), () -> fetcher.close(closeTimer), firstException);
    }

    Utils.closeQuietly(interceptors, "consumer interceptors", firstException);
    Utils.closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
    Utils.closeQuietly(metrics, "consumer metrics", firstException);
    Utils.closeQuietly(client, "consumer network client", firstException);
    Utils.closeQuietly(deserializers, "consumer deserializers", firstException);
    AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
    log.debug("Kafka share consumer has been closed");
    Throwable exception = firstException.get();
    if (exception != null && !swallowException) {
      if (exception instanceof InterruptException) {
        throw (InterruptException) exception;
      }
      throw new KafkaException("Failed to close kafka share consumer", exception);
    }
  }

  /**
   * Acquire the light lock and ensure that the consumer hasn't been closed.
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
   * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
   * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
   * supported).
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
    refcount.incrementAndGet();
  }

  /**
   * Release the light lock protecting the consumer from multi-threaded access.
   */
  private void release() {
    if (refcount.decrementAndGet() == 0)
      currentThread.set(NO_CURRENT_THREAD);
  }

  private void throwIfNoAssignorsConfigured() {
    if (assignors.isEmpty())
      throw new IllegalStateException("Must configure at least one partition assigner class name to " +
              ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
  }

  private void maybeThrowInvalidGroupIdException() {
    if (groupId.isEmpty())
      throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
              "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
  }

  private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
    if (offsetAndMetadata != null)
      offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
  }

  // Functions below are for testing only
  String getClientId() {
    return clientId;
  }

  boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
    return updateAssignmentMetadataIfNeeded(timer, true);
  }
}
