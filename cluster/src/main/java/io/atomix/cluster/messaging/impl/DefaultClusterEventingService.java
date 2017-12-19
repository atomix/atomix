/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ManagedClusterEventingService;
import io.atomix.cluster.messaging.Subscription;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingException;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Cluster event service.
 */
public class DefaultClusterEventingService implements ManagedClusterEventingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterEventingService.class);

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(NodeId.class)
      .register(LogicalTimestamp.class)
      .register(WallClockTimestamp.class)
      .register(SubscriptionMetadata.class)
      .register(InternalMessage.class)
      .register(InternalMessage.Type.class)
      .build());

  private static final String GOSSIP_MESSAGE_SUBJECT = "ClusterEventingService-update";

  private static final long GOSSIP_INTERVAL_MILLIS = 1000;
  private static final long TOMBSTONE_EXPIRATION_MILLIS = 1000 * 60;

  private final ClusterService clusterService;
  private final MessagingService messagingService;
  private final NodeId localNodeId;
  private final AtomicLong logicalTime = new AtomicLong();
  private ScheduledExecutorService gossipExecutor;
  private final Map<NodeId, Long> updateTimes = Maps.newConcurrentMap();
  private final Map<String, TopicSubscribers> topicSubscribers = Maps.newConcurrentMap();
  private final Map<String, List<SubscriptionMetadata>> topicSubscriptions = Maps.newConcurrentMap();
  private final Map<String, TopicIterator> topicIterators = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultClusterEventingService(ClusterService clusterService, MessagingService messagingService) {
    this.clusterService = clusterService;
    this.messagingService = messagingService;
    this.localNodeId = clusterService.getLocalNode().id();
  }

  @Override
  public <M> void broadcast(String topic, M message, Function<M, byte[]> encoder) {
    byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.ALL, encoder.apply(message)));
    getSubscriberNodes(topic).forEach(nodeId -> {
      Node node = clusterService.getNode(nodeId);
      if (node != null && node.getState() == Node.State.ACTIVE) {
        messagingService.sendAsync(node.endpoint(), topic, payload);
      }
    });
  }

  @Override
  public <M> CompletableFuture<Void> unicast(String topic, M message, Function<M, byte[]> encoder) {
    NodeId nodeId = getNextNodeId(topic);
    if (nodeId != null) {
      Node node = clusterService.getNode(nodeId);
      if (node != null && node.getState() == Node.State.ACTIVE) {
        byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.DIRECT, encoder.apply(message)));
        return messagingService.sendAsync(node.endpoint(), topic, payload);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<R> send(String topic, M message, Function<M, byte[]> encoder, Function<byte[], R> decoder) {
    NodeId nodeId = getNextNodeId(topic);
    if (nodeId != null) {
      Node node = clusterService.getNode(nodeId);
      if (node != null && node.getState() == Node.State.ACTIVE) {
        byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.DIRECT, encoder.apply(message)));
        return messagingService.sendAndReceive(node.endpoint(), topic, payload).thenApply(decoder);
      }
    }
    return Futures.exceptionalFuture(new MessagingException.NoRemoteHandler());
  }

  /**
   * Returns a collection of nodes that subscribe to the given topic.
   *
   * @param topic the topic for which to return the collection of subscriber nodes
   * @return the collection of subscribers for the given topic
   */
  private Stream<NodeId> getSubscriberNodes(String topic) {
    List<SubscriptionMetadata> subscribers = topicSubscriptions.get(topic);
    if (subscribers == null) {
      return Stream.empty();
    }
    return subscribers.stream()
        .filter(s -> !s.isTombstone())
        .map(s -> s.nodeId())
        .distinct();
  }

  /**
   * Returns the next node ID for the given message topic.
   *
   * @param topic the topic for which to return the next node ID
   * @return the next node ID for the given message topic
   */
  private NodeId getNextNodeId(String topic) {
    TopicIterator iterator = topicIterators.get(topic);
    return iterator != null && iterator.hasNext() ? iterator.next().nodeId() : null;
  }

  /**
   * Resets the iterator for the given message topic.
   *
   * @param topic the topic for which to reset the iterator
   */
  private synchronized void setSubscriberIterator(String topic) {
    List<SubscriptionMetadata> subscribers = topicSubscriptions.get(topic);
    if (subscribers == null) {
      topicIterators.remove(topic);
    } else {
      topicIterators.put(topic, new TopicIterator(subscribers));
    }
  }

  /**
   * Registers the node as a subscriber for the given topic.
   *
   * @param subscription the topic for which to register the node as a subscriber
   */
  private synchronized CompletableFuture<Subscription> registerSubscriber(InternalSubscription subscription) {
    TopicSubscribers subscribers = topicSubscribers.computeIfAbsent(subscription.topic(), t -> new TopicSubscribers(Collections.emptyList()));
    subscribers = subscribers.add(subscription);
    messagingService.registerHandler(subscription.topic(), subscribers);
    topicSubscribers.put(subscription.topic(), subscribers);
    List<SubscriptionMetadata> subscriptions = topicSubscriptions.computeIfAbsent(subscription.topic(), t -> Lists.newCopyOnWriteArrayList());
    subscriptions.add(subscription.metadata);
    return updateNodes().thenApply(v -> subscription);
  }

  /**
   * Unregisters the node as a subscriber for the given topic.
   */
  private synchronized CompletableFuture<Void> unregisterSubscriber(InternalSubscription subscription) {
    topicSubscribers.computeIfPresent(subscription.topic(), (topic, subscribers) -> {
      List<InternalSubscription> subscriptions = Arrays.asList(subscribers.subscriptions);
      if (subscriptions.remove(subscription)) {
        return new TopicSubscribers(subscriptions);
      }
      return subscribers;
    });

    List<SubscriptionMetadata> subscriptions = topicSubscriptions.get(subscription.topic());
    if (subscriptions != null) {
      // Create a new list of subscriptions with the given subscription changed to a tombstone.
      List<SubscriptionMetadata> newSubscriptions = subscriptions.stream()
          .map(s -> s == subscription.metadata ? s.asTombstone() : s)
          .collect(Collectors.toList());
      topicSubscriptions.put(subscription.topic(), newSubscriptions);

      // If all remaining subscriptions are tombstones, unsubscribe from the underlying topic.
      if (newSubscriptions.stream().filter(s -> s.isTombstone()).count() == 0) {
        messagingService.unregisterHandler(subscription.topic());
      }
      updateNodes();
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Subscription> subscribe(String topic, Function<byte[], M> decoder, Function<M, R> handler, Function<R, byte[]> encoder, Executor executor) {
    return registerSubscriber(new InternalSubscription(topic, payload -> {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      executor.execute(() -> {
        try {
          future.complete(encoder.apply(handler.apply(decoder.apply(payload))));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      });
      return future;
    }));
  }

  @Override
  public <M, R> CompletableFuture<Subscription> subscribe(String topic, Function<byte[], M> decoder, Function<M, CompletableFuture<R>> handler, Function<R, byte[]> encoder) {
    return registerSubscriber(new InternalSubscription(topic, payload -> {
      return handler.apply(decoder.apply(payload)).thenApply(encoder);
    }));
  }

  @Override
  public <M> CompletableFuture<Subscription> subscribe(String topic, Function<byte[], M> decoder, Consumer<M> handler, Executor executor) {
    return registerSubscriber(new InternalSubscription(topic, payload -> {
      executor.execute(() -> {
        try {
          handler.accept(decoder.apply(payload));
        } catch (Exception e) {
        }
      });
      return CompletableFuture.completedFuture(null);
    }));
  }

  @Override
  public List<Subscription> getSubscriptions(String topic) {
    TopicSubscribers subscribers = topicSubscribers.get(topic);
    if (subscribers == null) {
      return Collections.emptyList();
    }
    return ImmutableList.copyOf(subscribers.subscriptions);
  }

  /**
   * Handles a collection of subscription updates received via the gossip protocol.
   *
   * @param subscriptions a collection of subscriptions provided by the sender
   */
  private void update(Collection<SubscriptionMetadata> subscriptions) {
    for (SubscriptionMetadata subscription : subscriptions) {
      List<SubscriptionMetadata> topic = topicSubscriptions.computeIfAbsent(subscription.topic(), s -> Lists.newCopyOnWriteArrayList());
      SubscriptionMetadata matchingSubscription = topic.stream()
          .filter(s -> s.logicalTimestamp().equals(subscription.logicalTimestamp()))
          .findFirst()
          .orElse(null);
      if (matchingSubscription == null) {
        List<SubscriptionMetadata> newSubscriptions = Lists.newArrayList(topic);
        newSubscriptions.add(subscription);
        topicSubscriptions.put(subscription.topic(), newSubscriptions);
        setSubscriberIterator(subscription.topic());
      }
    }
  }

  /**
   * Sends a gossip message to an active peer.
   */
  private void gossip() {
    List<Node> nodes = clusterService.getNodes()
        .stream()
        .filter(node -> !localNodeId.equals(node.id()))
        .filter(node -> node.getState() == Node.State.ACTIVE)
        .collect(Collectors.toList());

    if (!nodes.isEmpty()) {
      Collections.shuffle(nodes);
      Node node = nodes.get(0);
      updateNode(node);
    }
  }

  /**
   * Updates all active peers with a given subscription.
   */
  private CompletableFuture<Void> updateNodes() {
    List<CompletableFuture<Void>> futures = clusterService.getNodes()
        .stream()
        .filter(node -> !localNodeId.equals(node.id()))
        .map(this::updateNode)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Sends an update to the given node.
   *
   * @param node the node to which to send the update
   */
  private CompletableFuture<Void> updateNode(Node node) {
    long updateTime = System.currentTimeMillis();
    long lastUpdateTime = updateTimes.getOrDefault(node.id(), 0L);

    Collection<SubscriptionMetadata> subscriptions = topicSubscriptions.values()
        .stream()
        .flatMap(s -> s.stream().filter(subscriber -> subscriber.timestamp().unixTimestamp() >= lastUpdateTime))
        .collect(Collectors.toList());

    CompletableFuture<Void> future = new CompletableFuture<>();
    messagingService.sendAsync(node.endpoint(), GOSSIP_MESSAGE_SUBJECT, SERIALIZER.encode(subscriptions))
        .whenComplete((result, error) -> {
          if (error == null) {
            updateTimes.put(node.id(), updateTime);
          }
          future.complete(null);
        });
    return future;
  }

  /**
   * Purges tombstones from the subscription list.
   */
  private void purgeTombstones() {
    long minTombstoneTime = clusterService.getNodes()
        .stream()
        .map(node -> updateTimes.getOrDefault(node.id(), 0L))
        .reduce(Math::min)
        .orElse(0L);
    for (List<SubscriptionMetadata> subscriptions : topicSubscriptions.values()) {
      subscriptions.removeIf(subscription -> subscription.isTombstone() && subscription.timestamp().unixTimestamp() < minTombstoneTime);
    }
  }

  @Override
  public CompletableFuture<ClusterEventingService> start() {
    if (started.compareAndSet(false, true)) {
      gossipExecutor = Executors.newSingleThreadScheduledExecutor(
          namedThreads("atomix-cluster-event-executor-%d", LOGGER));
      gossipExecutor.scheduleAtFixedRate(
          this::gossip,
          GOSSIP_INTERVAL_MILLIS,
          GOSSIP_INTERVAL_MILLIS,
          TimeUnit.MILLISECONDS);
      gossipExecutor.scheduleAtFixedRate(
          this::purgeTombstones,
          TOMBSTONE_EXPIRATION_MILLIS,
          TOMBSTONE_EXPIRATION_MILLIS,
          TimeUnit.MILLISECONDS);
      messagingService.registerHandler(GOSSIP_MESSAGE_SUBJECT, (endpoint, payload) -> {
        update(SERIALIZER.decode(payload));
      }, gossipExecutor);
      LOGGER.info("Started");
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      if (gossipExecutor != null) {
        gossipExecutor.shutdown();
      }
      LOGGER.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Internal message.
   */
  private static class InternalMessage {
    private enum Type {
      DIRECT,
      ALL,
    }

    private final Type type;
    private final byte[] payload;

    InternalMessage(Type type, byte[] payload) {
      this.type = type;
      this.payload = payload;
    }

    /**
     * Returns the message type.
     *
     * @return the message type
     */
    public Type type() {
      return type;
    }

    /**
     * Returns the payload.
     *
     * @return the payload
     */
    public byte[] payload() {
      return payload;
    }
  }

  /**
   * A set of internal subscribers.
   */
  private class TopicSubscribers implements BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> {
    private final AtomicInteger counter = new AtomicInteger();
    private final InternalSubscription[] subscriptions;
    private final int length;

    TopicSubscribers(Collection<InternalSubscription> subscriptions) {
      this.length = subscriptions.size();
      this.subscriptions = subscriptions.toArray(new InternalSubscription[length]);
    }

    /**
     * Returns the next subscription.
     *
     * @return the next subscription
     */
    private InternalSubscription next() {
      return subscriptions[counter.incrementAndGet() % length];
    }

    @Override
    public CompletableFuture<byte[]> apply(Endpoint endpoint, byte[] payload) {
      InternalMessage message = SERIALIZER.decode(payload);
      switch (message.type()) {
        case DIRECT:
          InternalSubscription subscription = next();
          return subscription.callback.apply(message.payload());
        case ALL:
        default:
          for (InternalSubscription s : subscriptions) {
            s.callback.apply(message.payload());
          }
          return CompletableFuture.completedFuture(null);
      }
    }

    TopicSubscribers add(InternalSubscription subscription) {
      List<InternalSubscription> subscriptions = new ArrayList<>(this.subscriptions.length + 1);
      subscriptions.addAll(Arrays.asList(this.subscriptions));
      subscriptions.add(subscription);
      return new TopicSubscribers(subscriptions);
    }
  }

  /**
   * Internal subscription.
   */
  private class InternalSubscription implements Subscription {
    private final SubscriptionMetadata metadata;
    private final Function<byte[], CompletableFuture<byte[]>> callback;

    public InternalSubscription(String topic, Function<byte[], CompletableFuture<byte[]>> callback) {
      this.metadata = new SubscriptionMetadata(localNodeId, topic, new LogicalTimestamp(logicalTime.incrementAndGet()));
      this.callback = callback;
    }

    @Override
    public String topic() {
      return metadata.topic();
    }

    @Override
    public CompletableFuture<Void> close() {
      return unregisterSubscriber(this);
    }
  }

  /**
   * Subscription metadata.
   */
  private static class SubscriptionMetadata {
    private final NodeId nodeId;
    private final String topic;
    private final LogicalTimestamp logicalTimestamp;
    private final boolean tombstone;
    private final WallClockTimestamp timestamp = new WallClockTimestamp();

    SubscriptionMetadata(NodeId nodeId, String topic, LogicalTimestamp logicalTimestamp) {
      this(nodeId, topic, logicalTimestamp, false);
    }

    SubscriptionMetadata(NodeId nodeId, String topic, LogicalTimestamp logicalTimestamp, boolean tombstone) {
      this.nodeId = nodeId;
      this.topic = topic;
      this.logicalTimestamp = logicalTimestamp;
      this.tombstone = tombstone;
    }

    public NodeId nodeId() {
      return nodeId;
    }

    public String topic() {
      return topic;
    }

    public LogicalTimestamp logicalTimestamp() {
      return logicalTimestamp;
    }

    public WallClockTimestamp timestamp() {
      return timestamp;
    }

    public boolean isTombstone() {
      return tombstone;
    }

    public SubscriptionMetadata asTombstone() {
      return new SubscriptionMetadata(nodeId, topic, logicalTimestamp, true);
    }
  }

  /**
   * Subscriber iterator that iterates subscribers in a loop.
   */
  private class TopicIterator implements Iterator<SubscriptionMetadata> {
    private final AtomicInteger counter = new AtomicInteger();
    private final SubscriptionMetadata[] subscribers;
    private final int length;

    TopicIterator(Collection<SubscriptionMetadata> subscribers) {
      this.length = subscribers.size();
      this.subscribers = subscribers.toArray(new SubscriptionMetadata[length]);
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public SubscriptionMetadata next() {
      return subscribers[counter.incrementAndGet() % length];
    }
  }
}
