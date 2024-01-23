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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeApplicationEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ShareFetchRequestManager implements RequestManager {

  private final Logger log;
  private final IdempotentCloser idempotentCloser = new IdempotentCloser();
  private final LogContext logContext;
  private final String groupId;
  private final ConsumerMetadata metadata;
  private final SubscriptionState subscriptions;
  private final FetchConfig fetchConfig;
  private final Time time;
  private final FetchMetricsManager metricsManager;
  private final ShareFetchBuffer fetchBuffer;
  private final BufferSupplier decompressionBufferSupplier;
  private final Set<Integer> nodesWithPendingFetchRequests;
  private final Set<Integer> nodesWithPendingAcknowledgeRequests;
  private final Map<Integer, ShareSessionHandler> sessionHandlers;
  private final Map<Node, ShareAcknowledgeApplicationEvent> pendingAcknowledgeEvents;
  private final Map<Node, CompletableFuture<Void>> inflightAcknowledgeFutures;

  ShareFetchRequestManager(final LogContext logContext,
                           final Time time,
                           final String groupId,
                           final ConsumerMetadata metadata,
                           final SubscriptionState subscriptions,
                           final FetchConfig fetchConfig,
                           final ShareFetchBuffer fetchBuffer,
                           final FetchMetricsManager metricsManager) {
    this.log = logContext.logger(ShareFetchRequestManager.class);
    this.logContext = logContext;
    this.time = time;
    this.groupId = groupId;
    this.metadata = metadata;
    this.subscriptions = subscriptions;
    this.fetchConfig = fetchConfig;
    this.fetchBuffer = fetchBuffer;
    this.metricsManager = metricsManager;
    this.decompressionBufferSupplier = BufferSupplier.create();
    this.nodesWithPendingFetchRequests = new HashSet<>();
    this.nodesWithPendingAcknowledgeRequests = new HashSet<>();
    this.sessionHandlers = new HashMap<>();
    this.pendingAcknowledgeEvents = new HashMap<>();
    this.inflightAcknowledgeFutures = new HashMap<>();
  }

  public CompletableFuture<Void> acknowledge(ShareAcknowledgeApplicationEvent ackEvent) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Node target = ackEvent.target();
    if (pendingAcknowledgeEvents.get(target.id()) != null) {
      // Can't handle multiple pending ack events for a single node
      future.completeExceptionally(new IllegalStateException("Multiple pending acks for node " + target));
    } else {
      pendingAcknowledgeEvents.put(target, ackEvent);
      inflightAcknowledgeFutures.put(target, future);
    }

    return future;
  }

  @Override
  public PollResult poll(long currentTimeMs) {
    List<UnsentRequest> requests;

    Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> shareAcknowledgeRequests = prepareShareAcknowledgeRequests();
    if (!shareAcknowledgeRequests.isEmpty()) {
      requests = shareAcknowledgeRequests.entrySet().stream().map(entry -> {
        final Node target = entry.getKey();
        final ShareSessionHandler.ShareAcknowledgeRequestData data = entry.getValue();
        final ShareAcknowledgeRequest.Builder request = createShareAcknowledgeRequest(target, data);
        final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
          if (error != null) {
            handleShareAcknowledgeFailure(target, data, error);
          } else {
            handleShareAcknowledgeSuccess(target, data, clientResponse);
          }
        };

        return new UnsentRequest(request, Optional.of(target)).whenComplete(responseHandler);
      }).collect(Collectors.toList());
    } else {
      Map<Node, ShareSessionHandler.ShareFetchRequestData> shareFetchRequests = prepareShareFetchRequests();

      requests = shareFetchRequests.entrySet().stream().map(entry -> {
        final Node target = entry.getKey();
        final ShareSessionHandler.ShareFetchRequestData data = entry.getValue();
        final ShareFetchRequest.Builder request = createShareFetchRequest(target, data);
        final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
          if (error != null) {
            handleShareFetchFailure(target, data, error);
          } else {
            handleShareFetchSuccess(target, data, clientResponse);
          }
        };

        return new UnsentRequest(request, Optional.of(target)).whenComplete(responseHandler);
      }).collect(Collectors.toList());
    }

    return new PollResult(requests);
  }

  private void handleShareFetchSuccess(Node fetchTarget,
                                       ShareSessionHandler.ShareFetchRequestData data,
                                       ClientResponse resp) {
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
        long fetchOffset = -1L;

        log.debug("Share fetch {} for partition {} returned fetch data {}",
                fetchConfig.isolationLevel, partition, partitionData);

        CompletedShareFetchNew completedFetch = new CompletedShareFetchNew(
                logContext,
                subscriptions,
                decompressionBufferSupplier,
                partition,
                partitionData,
                metricAggregator,
                requestVersion);
        fetchBuffer.add(completedFetch);
      }

      metricsManager.recordLatency(resp.requestLatencyMs());
    } finally {
      log.debug("Removing pending request for node {}", fetchTarget);
      nodesWithPendingFetchRequests.remove(fetchTarget.id());
    }
  }

  private void handleShareFetchFailure(Node fetchTarget,
                                       ShareSessionHandler.ShareFetchRequestData data,
                                       Throwable error) {
    try {
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

      if (handler != null) {
        handler.handleError(error);
      }
    } finally {
      nodesWithPendingFetchRequests.remove(fetchTarget.id());
    }
  }

  private Map<Node, ShareSessionHandler.ShareFetchRequestData> prepareShareFetchRequests() {
    Map<Node, ShareSessionHandler.Builder> fetchable = new HashMap<>();
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

//      if (client.isUnavailable(node)) {
//        client.maybeThrowAuthFailure(node);
//
//        // If we try to send during the reconnect backoff window, then the request is just
//        // going to be failed anyway before being sent, so skip sending the request for now
//        log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
//      } else
      if (nodesWithPendingFetchRequests.contains(node.id())) {
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

  private List<TopicPartition> fetchablePartitions() {
    return subscriptions.fetchablePartitions(tp -> true);
//    Set<TopicPartition> exclude = new HashSet<>();
//    if (nextInLineFetch != null && !nextInLineFetch.isConsumed) {
//      exclude.add(nextInLineFetch.partition.topicPartition());
//    }
//    for (CompletedShareFetch<K, V> completedFetch : completedFetches) {
//      exclude.add(completedFetch.partition.topicPartition());
//    }
//    return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
  }

  private ShareFetchRequest.Builder createShareFetchRequest(final Node fetchTarget,
                                                            final ShareSessionHandler.ShareFetchRequestData requestData) {
    final ShareFetchRequest.Builder request = ShareFetchRequest.Builder
            .forConsumer(ApiKeys.SHARE_FETCH.latestVersion(), fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
            .forShareSession(groupId, subscriptions, requestData.metadata())
            .setMaxBytes(fetchConfig.maxBytes)
            .removed(requestData.toForget())
            .replaced(requestData.toReplace());

    nodesWithPendingFetchRequests.add(fetchTarget.id());

    return request;
  }

  public ShareSessionHandler sessionHandler(int node) {
    return sessionHandlers.get(node);
  }

  private Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> prepareShareAcknowledgeRequests() {
    Map<Node, ShareSessionHandler.ShareAcknowledgeRequestData> reqs = new LinkedHashMap<>();
    pendingAcknowledgeEvents.forEach((target, ackEvent) -> {
      if (nodesWithPendingFetchRequests.contains(target.id())) {
        log.trace("Skipping ack because previous fetch request to {} has not been processed", target);
      } else if (nodesWithPendingAcknowledgeRequests.contains(target.id())) {
        log.trace("Skipping ack because previous acknowledge request to {} has not been processed", target);
      } else {
        reqs.put(target, ackEvent.acknowledgements());
      }
    });

    reqs.forEach((target, acks) -> {
      pendingAcknowledgeEvents.remove(target);
    });

    return reqs;
  }

  private ShareAcknowledgeRequest.Builder createShareAcknowledgeRequest(
          final Node target,
          final ShareSessionHandler.ShareAcknowledgeRequestData requestData) {
    // This code is a little simplified and makes a separate topic for each partition in the request that it builds
    // Eventually, the whole thing needs to be refactored, so it's more efficient to build the request from the information
    // the share session contains
    List<ShareAcknowledgeRequestData.AcknowledgeTopic> buildTopics = new ArrayList<>();

    Map<TopicIdPartition, List<ShareAcknowledgeRequest.AcknowledgementBatch>> sessionAcks = requestData.toSend();
    sessionAcks.forEach((topicIdPartition, sessionAckBatches) -> {
      ShareAcknowledgeRequestData.AcknowledgeTopic buildTopic = new ShareAcknowledgeRequestData.AcknowledgeTopic();
      buildTopic.setTopicId(topicIdPartition.topicId());
      ShareAcknowledgeRequestData.AcknowledgePartition buildPartition = new ShareAcknowledgeRequestData.AcknowledgePartition();
      buildPartition.setPartitionIndex(topicIdPartition.partition());
      List<ShareAcknowledgeRequestData.AcknowledgementBatch> buildAckBatches = new ArrayList<>();
      sessionAckBatches.forEach(sessionAckBatch -> {
        ShareAcknowledgeRequestData.AcknowledgementBatch buildAckBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
        buildAckBatch.setStartOffset(sessionAckBatch.startOffset);
        buildAckBatch.setLastOffset(sessionAckBatch.lastOffset);
        buildAckBatch.setAcknowledgeType(sessionAckBatch.type.toString().toLowerCase());
        buildAckBatches.add(buildAckBatch);
      });

      buildPartition.setAcknowledgementBatches(buildAckBatches);
      buildTopic.setPartitions(Collections.singletonList(buildPartition));
      buildTopics.add(buildTopic);
    });

    ShareSessionHandler sessionHandler = sessionHandler(target.id());

    final ShareAcknowledgeRequest.Builder request = new ShareAcknowledgeRequest.Builder(
            new ShareAcknowledgeRequestData()
                    .setSessionId(sessionHandler.sessionId())
                    .setSessionEpoch(sessionHandler.sessionEpoch())
                    .setTopics(buildTopics)
    );

    nodesWithPendingAcknowledgeRequests.add(target.id());

    return request;
  }

  private void handleShareAcknowledgeSuccess(Node fetchTarget,
                                             ShareSessionHandler.ShareAcknowledgeRequestData data,
                                             ClientResponse resp) {
    try {
      CompletableFuture<Void> future = inflightAcknowledgeFutures.remove(fetchTarget);
      future.complete(null);
    } finally {
      nodesWithPendingAcknowledgeRequests.remove(fetchTarget.id());
    }
  }

  private void handleShareAcknowledgeFailure(Node fetchTarget,
                                             ShareSessionHandler.ShareAcknowledgeRequestData data,
                                             Throwable error) {
    try {
      final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
      if (handler != null) {
        handler.handleError(error);
      }

      CompletableFuture<Void> future = inflightAcknowledgeFutures.get(fetchTarget);
      future.completeExceptionally(error);
    } finally {
      inflightAcknowledgeFutures.remove(fetchTarget);
      nodesWithPendingAcknowledgeRequests.remove(fetchTarget.id());
    }
  }

  public void close() {
    close(time.timer(Duration.ZERO));
  }
  public void close(final Timer timer) {
    idempotentCloser.close(() -> closeInternal(timer));
  }

  private void closeInternal(Timer timer) {
    Utils.closeQuietly(fetchBuffer, "fetchBuffer");
    Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
  }
}