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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ShareSessionHandler;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest.AcknowledgementBatch;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.*;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * {@code ShareFetcher} represents the basic state and logic for record fetching processing for a share group.
 * @param <K> Type for the message key
 * @param <V> Type for the message value
 */
public class ShareFetcher<K, V> implements Closeable {

  private final Logger log;
  protected final LogContext logContext;
  protected final ConsumerNetworkClient client;
  protected final String groupId;
  protected final ConsumerMetadata metadata;
  protected final SubscriptionState subscriptions;
  protected final FetchConfig fetchConfig;
  protected final Deserializers<K, V> deserializers;
  protected final Time time;
  protected final FetchMetricsManager metricsManager;

  private final BufferSupplier decompressionBufferSupplier;
  private final ConcurrentLinkedQueue<CompletedShareFetch<K, V>> completedFetches;
  private final Map<Integer, ShareSessionHandler> sessionHandlers;
  private final Set<Integer> nodesWithPendingFetchRequests;
  private final Set<Integer> nodesWithPendingAcknowledgeRequests;
  private CompletedShareFetch<K, V> nextInLineFetch;
  private Acknowledgements acknowledgements;

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public ShareFetcher(LogContext logContext,
                      ConsumerNetworkClient client,
                      String groupId,
                      ConsumerMetadata metadata,
                      SubscriptionState subscriptions,
                      FetchConfig fetchConfig,
                      Deserializers<K, V> deserializers,
                      FetchMetricsManager metricsManager,
                      Time time) {
    this.log = logContext.logger(ShareFetcher.class);
    this.logContext = logContext;
    this.client = client;
    this.groupId = groupId;
    this.metadata = metadata;
    this.subscriptions = subscriptions;
    this.fetchConfig = fetchConfig;
    this.deserializers = deserializers;
    this.decompressionBufferSupplier = BufferSupplier.create();
    this.completedFetches = new ConcurrentLinkedQueue<>();
    this.sessionHandlers = new HashMap<>();
    this.nodesWithPendingFetchRequests = new HashSet<>();
    this.nodesWithPendingAcknowledgeRequests = new HashSet<>();
    this.acknowledgements = Acknowledgements.empty();
    this.metricsManager = metricsManager;
    this.time = time;
  }

  /**
   * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
   * visibility for testing.
   *
   * @return true if there are completed fetches, false otherwise
   */
  boolean hasCompletedFetches() {
    return !completedFetches.isEmpty();
  }

  /**
   * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
   * @return true if there are completed fetches that can be returned, false otherwise
   */
  public boolean hasAvailableFetches() {
    return completedFetches.stream().anyMatch(fetch -> subscriptions.isFetchable(fetch.partition.topicPartition()));
  }

  /**
   * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
   * an in-flight fetch or pending fetch data.
   * @return number of fetches sent
   */
  public synchronized int sendFetches() {
    Map<Node, ShareSessionHandler.ShareFetchRequestData> fetchRequestMap = prepareShareFetchRequests();

    for (Map.Entry<Node, ShareSessionHandler.ShareFetchRequestData> entry : fetchRequestMap.entrySet()) {
      final Node fetchTarget = entry.getKey();
      final ShareSessionHandler.ShareFetchRequestData data = entry.getValue();
      final ShareFetchRequest.Builder request = createShareFetchRequest(fetchTarget, data);
      RequestFutureListener<ClientResponse> listener = new RequestFutureListener<ClientResponse>() {
        @Override
        public void onSuccess(ClientResponse resp) {
          synchronized (this) {
            handleFetchResponse(fetchTarget, data, resp);
          }
        }

        @Override
        public void onFailure(RuntimeException e) {
          synchronized (this) {
            handleFetchResponse(fetchTarget, e);
          }
        }
      };

      final RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
      future.addListener(listener);
    }

    return fetchRequestMap.size();
  }

  /**
   * Implements the core logic for a successful fetch request/response.
   *
   * @param fetchTarget {@link Node} from which the fetch data was requested
   * @param data {@link ShareSessionHandler.ShareFetchRequestData} that represents the session data
   * @param resp {@link ClientResponse} from which the {@link ShareFetchResponse} will be retrieved
   */
  protected void handleFetchResponse(final Node fetchTarget,
                                     final ShareSessionHandler.ShareFetchRequestData data,
                                     final ClientResponse resp) {
    try {
      final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

      if (handler == null) {
        log.error("Unable to find ShareSessionHandler for node {}. Ignoring fetch response.",
                fetchTarget.id());
        return;
      }

      final short requestVersion = resp.requestHeader().apiVersion();

      if (!handler.handleResponse(response, requestVersion)) {
        if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
          metadata.requestUpdate(false);
        }

        return;
      }

      final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), requestVersion);
      final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
      final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);

      for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
        TopicIdPartition partition = entry.getKey();
        ShareFetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);

        if (requestData == null) {
          String message;

          if (data.metadata().isFull()) {
            message = MessageFormatter.arrayFormat(
                    "Response for missing full request partition: partition={}; metadata={}",
                    new Object[]{partition, data.metadata()}).getMessage();
          } else {
            message = MessageFormatter.arrayFormat(
                    "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}; toReplace={}",
                    new Object[]{partition, data.metadata(), data.toSend(), data.toForget(), data.toReplace()}).getMessage();
          }

          // Received fetch response for missing session partition
          throw new IllegalStateException(message);
        }

        ShareFetchResponseData.PartitionData partitionData = entry.getValue();

        log.debug("Share fetch {} for partition {} returned fetch data {}",
                fetchConfig.isolationLevel, partition, partitionData);

        CompletedShareFetch<K, V> completedFetch = new CompletedShareFetch<>(
                logContext,
                subscriptions,
                fetchConfig,
                deserializers,
                decompressionBufferSupplier,
                partition,
                partitionData,
                metricAggregator,
                requestVersion);
        completedFetches.add(completedFetch);
      }

      metricsManager.recordLatency(resp.requestLatencyMs());
    } finally {
      log.debug("Removing pending request for node {}", fetchTarget);
      nodesWithPendingFetchRequests.remove(fetchTarget.id());
    }
  }

  /**
   * Implements the core logic for a failed fetch request/response.
   *
   * @param fetchTarget {@link Node} from which the fetch data was requested
   * @param e {@link RuntimeException} representing the error that resulted in the failure
   */
  protected void handleFetchResponse(final Node fetchTarget, final RuntimeException e) {
    try {
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

      if (handler != null) {
        handler.handleError(e);
      }
    } finally {
      log.debug("Removing pending request for node {}", fetchTarget);
      nodesWithPendingFetchRequests.remove(fetchTarget.id());
    }
  }

  /**
   * Creates a new {@link ShareFetchRequest fetch request} in preparation for sending to the Kafka cluster.
   *
   * @param fetchTarget {@link Node} from which the fetch data will be requested
   * @param requestData {@link ShareSessionHandler.ShareFetchRequestData} that represents the session data
   * @return {@link ShareFetchRequest.Builder} that can be submitted to the broker
   */
  protected ShareFetchRequest.Builder createShareFetchRequest(final Node fetchTarget,
                                                    final ShareSessionHandler.ShareFetchRequestData requestData) {
    final ShareFetchRequest.Builder request = ShareFetchRequest.Builder
            .forConsumer(ApiKeys.SHARE_FETCH.latestVersion(), fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
            .forShareSession(groupId, subscriptions, requestData.metadata())
            .setMaxBytes(fetchConfig.maxBytes)
            .removed(requestData.toForget())
            .replaced(requestData.toReplace());

    log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget);

    // We add the node to the set of nodes with pending fetch requests before adding the
    // listener because the future may have been fulfilled on another thread (e.g. during a
    // disconnection being handled by the heartbeat thread) which will mean the listener
    // will be invoked synchronously.
    log.debug("Adding pending request for node {}", fetchTarget);
    nodesWithPendingFetchRequests.add(fetchTarget.id());

    return request;
  }

  /**
   * Return the fetched records, empty the record buffer and update the consumed position.
   *
   * </p>
   *
   * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
   *
   * @return A {@link Fetch} for the requested partitions
   * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
   *         the defaultResetPolicy is NONE
   * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
   */
  public Fetch<K, V> collectFetch() {
    Fetch<K, V> fetch = Fetch.empty();
    Queue<CompletedShareFetch<K, V>> pausedCompletedFetches = new ArrayDeque<>();
    int recordsRemaining = fetchConfig.maxPollRecords;

    try {
      while (recordsRemaining > 0) {
        if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
          CompletedShareFetch<K, V> records = completedFetches.peek();
          if (records == null) break;

          if (!records.initialized) {
            try {
              nextInLineFetch = initializeCompletedShareFetch(records);
            } catch (Exception e) {
              // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
              // (2) there are no fetched records with actual content preceding this exception.
              // The first condition ensures that the completedFetches is not stuck with the same completedFetch
              // in cases such as the TopicAuthorizationException, and the second condition ensures that no
              // potential data loss due to an exception in a following record.
              if (fetch.isEmpty() && ShareFetchResponse.recordsOrFail(records.partitionData).sizeInBytes() == 0) {
                completedFetches.poll();
              }
              throw e;
            }
          } else {
            nextInLineFetch = records;
          }
          completedFetches.poll();
        } else {
          Fetch<K, V> nextFetch = fetchRecords(recordsRemaining);
          recordsRemaining -= nextFetch.numRecords();
          fetch.add(nextFetch);
        }
      }
    } catch (KafkaException e) {
      if (fetch.isEmpty())
        throw e;
    } finally {
      // add any polled completed fetches for paused partitions back to the completed fetches queue to be
      // re-evaluated in the next poll
      completedFetches.addAll(pausedCompletedFetches);
    }

    return fetch;
  }

  private Fetch<K, V> fetchRecords(final int maxRecords) {
    if (!subscriptions.isAssigned(nextInLineFetch.partition.topicPartition())) {
      // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
      log.debug("Not returning fetched records for partition {} since it is no longer assigned",
              nextInLineFetch.partition);
    } else if (!subscriptions.isFetchable(nextInLineFetch.partition.topicPartition())) {
      // this can happen when a partition is paused before fetched records are returned to the consumer's
      // poll call or if the offset is being reset
      log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
              nextInLineFetch.partition);
    } else {
        List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(maxRecords);

        log.trace("Returning {} fetched records for assigned partition {}",
                partRecords.size(), nextInLineFetch.partition);

        return Fetch.forPartition(nextInLineFetch.partition.topicPartition(), partRecords, true);
    }

    log.trace("Draining fetched records for partition {}", nextInLineFetch.partition);
    nextInLineFetch.drain();

    return Fetch.empty();
  }

  private List<TopicPartition> fetchablePartitions() {
    Set<TopicPartition> exclude = new HashSet<>();
    if (nextInLineFetch != null && !nextInLineFetch.isConsumed) {
      exclude.add(nextInLineFetch.partition.topicPartition());
    }
    for (CompletedShareFetch<K, V> completedFetch : completedFetches) {
      exclude.add(completedFetch.partition.topicPartition());
    }
    return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
  }

  /**
   * Create fetch requests for all nodes for which we have assigned partitions
   * that have no existing requests in flight.
   */
  protected Map<Node, ShareSessionHandler.ShareFetchRequestData> prepareShareFetchRequests() {
    // Update metrics in case there was an assignment change
    metricsManager.maybeUpdateAssignment(subscriptions);

    Map<Node, ShareSessionHandler.Builder> fetchable = new LinkedHashMap<>();
    long currentTimeMs = time.milliseconds();
    Map<String, Uuid> topicIds = metadata.topicIds();

    for (TopicPartition partition : fetchablePartitions()) {
      SubscriptionState.FetchPosition position = subscriptions.position(partition);

      if (position == null)
        throw new IllegalStateException("Missing position for fetchable partition " + partition);

      Optional<Node> leaderOpt = position.currentLeader.leader;

      if (!leaderOpt.isPresent()) {
        log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
        metadata.requestUpdate(false);
        continue;
      }

      Node node = leaderOpt.get();

      if (client.isUnavailable(node)) {
        client.maybeThrowAuthFailure(node);

        // If we try to send during the reconnect backoff window, then the request is just
        // going to be failed anyway before being sent, so skip sending the request for now
        log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
      } else if (nodesWithPendingFetchRequests.contains(node.id())) {
        log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node);
      } else if (nodesWithPendingAcknowledgeRequests.contains(node.id())) {
        log.trace("Skipping fetch for partition {} because previous acknowledge request to {} has not been processed", partition, node);
      } else {
        // if there is a leader and no in-flight requests, issue a new fetch
        ShareSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
          ShareSessionHandler shareSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n));
          return shareSessionHandler.newBuilder();
        });
        Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
        ShareFetchRequest.PartitionData partitionData = new ShareFetchRequest.PartitionData(
                topicId,
                partition.partition(),
                fetchConfig.fetchSize,
                position.currentLeader.epoch);
        builder.add(new TopicIdPartition(topicId, partition), partitionData);

        log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchConfig.isolationLevel,
                partition, position, node);
      }
    }

    Map<Node, ShareSessionHandler.ShareFetchRequestData> reqs = new LinkedHashMap<>();
    for (Map.Entry<Node, ShareSessionHandler.Builder> entry : fetchable.entrySet()) {
      reqs.put(entry.getKey(), entry.getValue().build());
    }

    // Because ShareFetcher doesn't have the concept of a validated position, we need to
    // do some fakery here to keep the underlying fetching state management happy. This
    // is a sign of a future refactor.
    if (reqs.isEmpty()) {
      for (TopicPartition tp : subscriptions.initializingPartitions()) {
        Optional<Node> leader = metadata.currentLeader(tp).leader;
        if (leader.isPresent()) {
          subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp)));
          subscriptions.maybeValidatePositionForCurrentLeader(new ApiVersions(), tp, metadata.currentLeader(tp));
        }
      }
    }

    return reqs;
  }

  /**
   * Initialize a CompletedFetch object.
   */
  private CompletedShareFetch<K, V> initializeCompletedShareFetch(final CompletedShareFetch<K, V> completedFetch) {
    final TopicIdPartition tp = completedFetch.partition;
    final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());
    boolean recordMetrics = true;

    try {
      if (!subscriptions.hasValidPosition(tp.topicPartition())) {
        // this can happen when a rebalance happened while fetch is still in-flight
        log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
        return null;
      } else if (error == Errors.NONE) {
        final CompletedShareFetch<K, V> ret = handleInitializeCompletedShareFetchSuccess(completedFetch);
        recordMetrics = ret == null;
        return ret;
      } else {
        handleInitializeCompletedShareFetchErrors(completedFetch, error);
        return null;
      }
    } finally {
      if (recordMetrics) {
        completedFetch.recordAggregatedMetrics(0, 0);
      }

      if (error != Errors.NONE)
        // we move the partition to the end if there was an error. This way, it's more likely that partitions for
        // the same topic can remain together (allowing for more efficient serialization).
        subscriptions.movePartitionToEnd(tp.topicPartition());
    }
  }

  private CompletedShareFetch<K, V> handleInitializeCompletedShareFetchSuccess(final CompletedShareFetch<K, V> completedShareFetch) {
    final TopicIdPartition tp = completedShareFetch.partition;

    final ShareFetchResponseData.PartitionData partition = completedShareFetch.partitionData;
    log.trace("Preparing to read {} bytes of data for partition {}",
            ShareFetchResponse.recordsSize(partition), tp);
    Iterator<? extends RecordBatch> batches = ShareFetchResponse.recordsOrFail(partition).batches().iterator();

    if (!batches.hasNext() && ShareFetchResponse.recordsSize(partition) > 0) {
      throw new KafkaException("Failed to make progress reading messages at " + tp +
              ". Received a non-empty fetch response from the server, but no " +
              "complete records were found.");
    }

    completedShareFetch.initialized = true;
    return completedShareFetch;
  }

  private void handleInitializeCompletedShareFetchErrors(final CompletedShareFetch<K, V> completedShareFetch,
                                                    final Errors error) {
    final TopicIdPartition tp = completedShareFetch.partition;

    if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
            error == Errors.REPLICA_NOT_AVAILABLE ||
            error == Errors.KAFKA_STORAGE_ERROR ||
            error == Errors.FENCED_LEADER_EPOCH ||
            error == Errors.OFFSET_NOT_AVAILABLE) {
      log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
      requestMetadataUpdate(tp);
    } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
      log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
      requestMetadataUpdate(tp);
    } else if (error == Errors.UNKNOWN_TOPIC_ID) {
      log.warn("Received unknown topic ID error in fetch for partition {}", tp);
      requestMetadataUpdate(tp);
    } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
      log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
      requestMetadataUpdate(tp);
    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
      //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
      log.warn("Not authorized to read from partition {}.", tp);
      throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
    } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
      log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
    } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
      log.warn("Unknown server error while fetching for topic-partition {}", tp);
    } else if (error == Errors.CORRUPT_MESSAGE) {
      throw new KafkaException("Encountered corrupt message when fetching "
              + " for topic-partition "
              + tp);
    } else {
      throw new IllegalStateException("Unexpected error code "
              + error.code()
              + " fetching from topic-partition " + tp);
    }
  }

  /**
   * Clear the buffered data which are not a part of newly assigned partitions. Any previously
   * {@link CompletedFetch fetched data} is dropped if it is for a partition that is no longer in
   * {@code assignedPartitions}.
   *
   * @param assignedPartitions Newly-assigned {@link TopicPartition}
   */
  public void clearBufferedDataForUnassignedPartitions(final Collection<TopicPartition> assignedPartitions) {
    final Iterator<CompletedShareFetch<K, V>> completedFetchesItr = completedFetches.iterator();

    while (completedFetchesItr.hasNext()) {
      final CompletedShareFetch<K, V> completedFetch = completedFetchesItr.next();
      final TopicPartition tp = completedFetch.partition.topicPartition();

      if (!assignedPartitions.contains(tp)) {
        log.debug("Removing {} from buffered data as it is no longer an assigned partition", tp);
        completedFetch.drain();
        completedFetchesItr.remove();
      }
    }

    if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition)) {
      nextInLineFetch.drain();
      nextInLineFetch = null;
    }
  }

  /**
   * Clear the buffered data which are not a part of newly assigned topics
   *
   * @param assignedTopics  newly assigned topics
   */
  public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
    final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

    for (TopicPartition tp : subscriptions.assignedPartitions()) {
      if (assignedTopics.contains(tp.topic())) {
        currentTopicPartitions.add(tp);
      }
    }

    clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
  }

  /**
   * Sends a ShareAcknowledge request for all uncommitted acknowledgements.
   */
  public synchronized int sendAcknowledgements() {
    Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> acknowledgeRequestMap = prepareShareAcknowledgeRequests(acknowledgements);

    for (Map.Entry<Node, ShareSessionHandler.ShareAcknowledgeRequestData> entry : acknowledgeRequestMap.entrySet()) {
      final Node acknowledgeTarget = entry.getKey();
      final ShareSessionHandler.ShareAcknowledgeRequestData data = entry.getValue();
      final ShareAcknowledgeRequest.Builder request = createShareAcknowledgeRequest(acknowledgeTarget, data);
      RequestFutureListener<ClientResponse> listener = new RequestFutureListener<ClientResponse>() {
        @Override
        public void onSuccess(ClientResponse resp) {
          synchronized (this) {
            handleAcknowledgeResponse(acknowledgeTarget, data, resp);
          }
        }

        @Override
        public void onFailure(RuntimeException e) {
          synchronized (this) {
            handleAcknowledgeResponse(acknowledgeTarget, e);
          }
        }
      };

      final RequestFuture<ClientResponse> future = client.send(acknowledgeTarget, request);
      future.addListener(listener);
    }

    return acknowledgeRequestMap.size();
  }

  /**
   * Create fetch requests for all nodes for which we have assigned partitions
   * that have no existing requests in flight.
   */
  protected Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> prepareShareAcknowledgeRequests(Acknowledgements acknowledgements) {
    // Update metrics in case there was an assignment change
    metricsManager.maybeUpdateAssignment(subscriptions);

    Map<Node, ShareSessionHandler.AcknowledgeBuilder> fetchable = new LinkedHashMap<>();

    for (TopicIdPartition partition : acknowledgements.partitions()) {
      SubscriptionState.FetchPosition position = subscriptions.position(partition.topicPartition());

      if (position == null)
        throw new IllegalStateException("Missing position for fetchable partition " + partition);

      Optional<Node> leaderOpt = position.currentLeader.leader;

      if (!leaderOpt.isPresent()) {
        log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
        metadata.requestUpdate(false);
        continue;
      }

      Node node = leaderOpt.get();

      if (client.isUnavailable(node)) {
        client.maybeThrowAuthFailure(node);

        // If we try to send during the reconnect backoff window, then the request is just
        // going to be failed anyway before being sent, so skip sending the request for now
        log.trace("Skipping acknowledge for partition {} because node {} is awaiting reconnect backoff", partition, node);
      } else if (nodesWithPendingAcknowledgeRequests.contains(node.id())) {
        log.trace("Skipping acknowledge for partition {} because previous acknowledge request to {} has not been processed", partition, node);
      } else if (nodesWithPendingFetchRequests.contains(node.id())) {
        log.trace("Skipping acknowledge for partition {} because previous fetch request to {} has not been processed", partition, node);
      } else {
        // Build an AcknowledgementBatch for the topic-partition
        Map<Long, AcknowledgeType> ackMap = acknowledgements.forPartition(partition);
        if (ackMap != null) {
          // if there is a leader and no in-flight requests, issue a new request
          ShareSessionHandler.AcknowledgeBuilder builder = fetchable.computeIfAbsent(node, k -> {
            ShareSessionHandler shareSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n));
            return shareSessionHandler.newAcknowledgeBuilder();
          });

          AcknowledgementBatch ackBatch = null;
          for (Map.Entry<Long, AcknowledgeType> e: ackMap.entrySet()) {
            if (ackBatch == null) {
              ackBatch = new AcknowledgementBatch(e.getKey(), e.getKey(), e.getValue());
            } else {
              if ((e.getKey() == ackBatch.lastOffset + 1) && (e.getValue() == ackBatch.type)) {
                ackBatch = new AcknowledgementBatch(ackBatch.startOffset, e.getKey(), ackBatch.type);
              } else {
                builder.add(partition, ackBatch);
                ackBatch = new AcknowledgementBatch(e.getKey(), e.getKey(), e.getValue());
              }
            }
          }
          if (ackBatch != null) {
            builder.add(partition, ackBatch);
          }

          acknowledgements.clearForPartition(partition);

          log.debug("Added acknowledge request for partition {} to node {}", partition, node);
        }
      }
    }

    Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> reqs = new LinkedHashMap<>();
    for (Map.Entry<Node, ShareSessionHandler.AcknowledgeBuilder> entry : fetchable.entrySet()) {
      reqs.put(entry.getKey(), entry.getValue().build());
    }

    return reqs;
  }

  protected ShareAcknowledgeRequest.Builder createShareAcknowledgeRequest(final Node fetchTarget,
                                                              final ShareSessionHandler.ShareAcknowledgeRequestData requestData) {
    final ShareAcknowledgeRequest.Builder request = ShareAcknowledgeRequest.Builder
            .forConsumer(requestData.toSend())
            .forShareSession(requestData.metadata());

    log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget);

    // We add the node to the set of nodes with pending fetch requests before adding the
    // listener because the future may have been fulfilled on another thread (e.g. during a
    // disconnection being handled by the heartbeat thread) which will mean the listener
    // will be invoked synchronously.
    log.debug("Adding pending request for node {}", fetchTarget);
    nodesWithPendingAcknowledgeRequests.add(fetchTarget.id());

    return request;
  }

  protected void handleAcknowledgeResponse(final Node fetchTarget,
                                     final ShareSessionHandler.ShareAcknowledgeRequestData data,
                                     final ClientResponse resp) {
    try {
      final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

      if (handler == null) {
        log.error("Unable to find ShareSessionHandler for node {}. Ignoring acknowledge response.",
                fetchTarget.id());
        return;
      }

      final short requestVersion = resp.requestHeader().apiVersion();

      if (!handler.handleResponse(response, requestVersion)) {
        if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
          metadata.requestUpdate(false);
        }

        return;
      }

      metricsManager.recordLatency(resp.requestLatencyMs());
    } finally {
      log.debug("Removing pending request for node {}", fetchTarget);
      nodesWithPendingAcknowledgeRequests.remove(fetchTarget.id());
    }
  }

  public void mergeAcknowledgements(Acknowledgements additional) {
    acknowledgements.merge(additional);
  }

  public boolean hasAcknowledgeRequestsInFlight() {
    return !(nodesWithPendingAcknowledgeRequests.isEmpty() && acknowledgements.isEmpty());
  }

  protected void handleAcknowledgeResponse(final Node fetchTarget, final RuntimeException e) {
    try {
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

      if (handler != null) {
        handler.handleError(e);
      }
    } finally {
      log.debug("Removing pending request for node {}", fetchTarget);
      nodesWithPendingAcknowledgeRequests.remove(fetchTarget.id());
    }
  }

  protected ShareSessionHandler sessionHandler(int node) {
    return sessionHandlers.get(node);
  }

  // Visible for testing
  void maybeCloseFetchSessions(final Timer timer) {
    final Cluster cluster = metadata.fetch();
    final List<RequestFuture<ClientResponse>> requestFutures = new ArrayList<>();

    sessionHandlers.forEach((fetchTargetNodeId, sessionHandler) -> {
      // set the session handler to notify close. This will set the next metadata request to send close message.
      sessionHandler.notifyClose();

      final int sessionId = sessionHandler.sessionId();
      // FetchTargetNode may not be available as it may have disconnected the connection. In such cases, we will
      // skip sending the close request.
      final Node fetchTarget = cluster.nodeById(fetchTargetNodeId);
      if (fetchTarget == null || client.isUnavailable(fetchTarget)) {
        log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget);
        return;
      }

      // TO-DO: Needs to release acquired records
      final ShareFetchRequest.Builder request = createShareFetchRequest(fetchTarget, sessionHandler.newBuilder().build());
      final RequestFuture<ClientResponse> responseFuture = client.send(fetchTarget, request);

      responseFuture.addListener(new RequestFutureListener<ClientResponse>() {
        @Override
        public void onSuccess(ClientResponse value) {
          log.debug("Successfully sent a close message for fetch session: {} to node: {}", sessionId, fetchTarget);
        }

        @Override
        public void onFailure(RuntimeException e) {
          log.debug("Unable to a close message for fetch session: {} to node: {}. " +
                  "This may result in unnecessary fetch sessions at the broker.", sessionId, fetchTarget, e);
        }
      });

      requestFutures.add(responseFuture);
    });

    // Poll to ensure that request has been written to the socket. Wait until either the timer has expired or until
    // all requests have received a response.
    while (timer.notExpired() && !requestFutures.stream().allMatch(RequestFuture::isDone)) {
      client.poll(timer, null, true);
    }

    if (!requestFutures.stream().allMatch(RequestFuture::isDone)) {
      // we ran out of time before completing all futures. It is ok since we don't want to block the shutdown
      // here.
      log.debug("All requests couldn't be sent in the specific timeout period {}ms. " +
              "This may result in unnecessary fetch sessions at the broker. Consider increasing the timeout passed for " +
              "KafkaConsumer.close(Duration timeout)", timer.timeoutMs());
    }
  }

  public void close(final Timer timer) {
    if (!isClosed.compareAndSet(false, true)) {
      log.info("Fetcher {} is already closed.", this);
      return;
    }

    // Shared states (e.g. sessionHandlers) could be accessed by multiple threads (such as heartbeat thread), hence,
    // it is necessary to acquire a lock on the fetcher instance before modifying the states.
    // we do not need to re-enable wakeups since we are closing already
    synchronized (this) {
      client.disableWakeups();

      if (nextInLineFetch != null) {
        nextInLineFetch.drain();
        nextInLineFetch = null;
      }

      maybeCloseFetchSessions(timer);
      Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
      sessionHandlers.clear();
    }
  }

  @Override
  public void close() {
    close(time.timer(0));
  }

  private void requestMetadataUpdate(final TopicIdPartition topicPartition) {
    metadata.requestUpdate(false);
  }
}