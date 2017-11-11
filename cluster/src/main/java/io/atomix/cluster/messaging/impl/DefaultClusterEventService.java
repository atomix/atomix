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

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.Node.State;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ManagedClusterEventService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.messaging.MessagingException;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Cluster event service.
 */
public class DefaultClusterEventService implements ManagedClusterEventService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterEventService.class);

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(NodeId.class)
      .register(Subscription.class)
      .register(MessageSubject.class)
      .register(LogicalTimestamp.class)
      .register(WallClockTimestamp.class)
      .build());

  private static final MessageSubject GOSSIP_MESSAGE_SUBJECT = new MessageSubject("ClusterEventService-update");

  private static final long GOSSIP_INTERVAL_MILLIS = 1000;
  private static final long TOMBSTONE_EXPIRATION_MILLIS = 1000 * 60;

  private final ClusterService clusterService;
  private final ClusterCommunicationService clusterCommunicator;
  private final NodeId localNodeId;
  private final AtomicLong logicalTime = new AtomicLong();
  private ScheduledExecutorService gossipExecutor;
  private final Map<NodeId, Long> updateTimes = Maps.newConcurrentMap();
  private final Map<MessageSubject, Map<NodeId, Subscription>> subjectSubscriptions = Maps.newConcurrentMap();
  private final Map<MessageSubject, SubscriberIterator> subjectIterators = Maps.newConcurrentMap();
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultClusterEventService(ClusterService clusterService, ClusterCommunicationService clusterCommunicator) {
    this.clusterService = clusterService;
    this.clusterCommunicator = clusterCommunicator;
    this.localNodeId = clusterService.getLocalNode().id();
  }

  @Override
  public <M> void broadcast(MessageSubject subject, M message, Function<M, byte[]> encoder) {
    Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
    if (subscribers != null) {
      subscribers.forEach(nodeId -> clusterCommunicator.unicast(subject, message, encoder, nodeId));
    }
  }

  @Override
  public <M> CompletableFuture<Void> unicast(MessageSubject subject, M message, Function<M, byte[]> encoder) {
    NodeId nodeId = getNextNodeId(subject);
    if (nodeId != null) {
      return clusterCommunicator.unicast(subject, message, encoder, nodeId);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<R> sendAndReceive(MessageSubject subject, M message, Function<M, byte[]> encoder, Function<byte[], R> decoder) {
    NodeId nodeId = getNextNodeId(subject);
    if (nodeId == null) {
      return Futures.exceptionalFuture(new MessagingException.NoRemoteHandler());
    }
    return clusterCommunicator.sendAndReceive(subject, message, encoder, decoder, nodeId);
  }

  /**
   * Returns the set of nodes with subscribers for the given message subject.
   *
   * @param subject the subject for which to return a set of nodes
   * @return a set of nodes with subscribers for the given subject
   */
  private Collection<? extends NodeId> getSubscriberNodes(MessageSubject subject) {
    Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
    if (nodeSubscriptions == null) {
      return null;
    }
    return nodeSubscriptions.values()
        .stream()
        .filter(s -> {
          Node node = clusterService.getNode(s.nodeId());
          return node != null && node.state() == State.ACTIVE && !s.isTombstone();
        })
        .map(s -> s.nodeId())
        .collect(Collectors.toList());
  }

  /**
   * Returns the next node ID for the given message subject.
   *
   * @param subject the subject for which to return the next node ID
   * @return the next node ID for the given message subject
   */
  private NodeId getNextNodeId(MessageSubject subject) {
    SubscriberIterator iterator = subjectIterators.get(subject);
    return iterator != null && iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Resets the iterator for the given message subject.
   *
   * @param subject the subject for which to reset the iterator
   */
  private synchronized void setSubscriberIterator(MessageSubject subject) {
    Collection<? extends NodeId> subscriberNodes = getSubscriberNodes(subject);
    if (subscriberNodes != null && !subscriberNodes.isEmpty()) {
      subjectIterators.put(subject, new SubscriberIterator(subscriberNodes));
    } else {
      subjectIterators.remove(subject);
    }
  }

  /**
   * Registers the node as a subscriber for the given subject.
   *
   * @param subject the subject for which to register the node as a subscriber
   */
  private synchronized CompletableFuture<Void> registerSubscriber(MessageSubject subject) {
    Map<NodeId, Subscription> nodeSubscriptions =
        subjectSubscriptions.computeIfAbsent(subject, s -> Maps.newConcurrentMap());
    Subscription subscription = new Subscription(
        localNodeId,
        subject,
        new LogicalTimestamp(logicalTime.incrementAndGet()));
    nodeSubscriptions.put(localNodeId, subscription);
    return updateNodes();
  }

  /**
   * Unregisters the node as a subscriber for the given subject.
   *
   * @param subject the subject for which to unregister the node as a subscriber
   */
  private synchronized void unregisterSubscriber(MessageSubject subject) {
    Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
    if (nodeSubscriptions != null) {
      Subscription subscription = nodeSubscriptions.get(localNodeId);
      if (subscription != null) {
        nodeSubscriptions.put(localNodeId, subscription.asTombstone());
        updateNodes();
      }
    }
  }

  @Override
  public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Function<M, R> handler, Function<R, byte[]> encoder, Executor executor) {
    return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder, executor)
        .thenCompose(v -> registerSubscriber(subject));
  }

  @Override
  public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Function<M, CompletableFuture<R>> handler, Function<R, byte[]> encoder) {
    registerSubscriber(subject);
    return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder)
        .thenCompose(v -> registerSubscriber(subject));
  }

  @Override
  public <M> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Consumer<M> handler, Executor executor) {
    return clusterCommunicator.addSubscriber(subject, decoder, handler, executor)
        .thenCompose(v -> registerSubscriber(subject));
  }

  @Override
  public void removeSubscriber(MessageSubject subject) {
    unregisterSubscriber(subject);
    clusterCommunicator.removeSubscriber(subject);
  }

  /**
   * Handles a collection of subscription updates received via the gossip protocol.
   *
   * @param subscriptions a collection of subscriptions provided by the sender
   */
  private void update(Collection<Subscription> subscriptions) {
    for (Subscription subscription : subscriptions) {
      Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.computeIfAbsent(
          subscription.subject(), s -> Maps.newConcurrentMap());
      Subscription existingSubscription = nodeSubscriptions.get(subscription.nodeId());
      if (existingSubscription == null
          || existingSubscription.logicalTimestamp().isOlderThan(subscription.logicalTimestamp())) {
        nodeSubscriptions.put(subscription.nodeId(), subscription);
        setSubscriberIterator(subscription.subject());
      }
    }
  }

  /**
   * Sends a gossip message to an active peer.
   */
  private void gossip() {
    List<NodeId> nodes = clusterService.getNodes()
        .stream()
        .filter(node -> !localNodeId.equals(node.id()))
        .filter(node -> node.state() == State.ACTIVE)
        .map(Node::id)
        .collect(Collectors.toList());

    if (!nodes.isEmpty()) {
      Collections.shuffle(nodes);
      NodeId node = nodes.get(0);
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
        .map(Node::id)
        .map(this::updateNode)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Sends an update to the given node.
   *
   * @param nodeId the node to which to send the update
   */
  private CompletableFuture<Void> updateNode(NodeId nodeId) {
    long updateTime = System.currentTimeMillis();
    long lastUpdateTime = updateTimes.getOrDefault(nodeId.id(), 0L);

    Collection<Subscription> subscriptions = new ArrayList<>();
    subjectSubscriptions.values().forEach(ns -> ns.values()
        .stream()
        .filter(subscription -> subscription.timestamp().unixTimestamp() >= lastUpdateTime)
        .forEach(subscriptions::add));

    CompletableFuture<Void> future = new CompletableFuture<>();
    clusterCommunicator.sendAndReceive(GOSSIP_MESSAGE_SUBJECT, subscriptions, SERIALIZER::encode, SERIALIZER::decode, nodeId)
        .whenComplete((result, error) -> {
          if (error == null) {
            updateTimes.put(nodeId, updateTime);
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
    for (Map<NodeId, Subscription> nodeSubscriptions : subjectSubscriptions.values()) {
      Iterator<Map.Entry<NodeId, Subscription>> nodeSubscriptionIterator =
          nodeSubscriptions.entrySet().iterator();
      while (nodeSubscriptionIterator.hasNext()) {
        Subscription subscription = nodeSubscriptionIterator.next().getValue();
        if (subscription.isTombstone() && subscription.timestamp().unixTimestamp() < minTombstoneTime) {
          nodeSubscriptionIterator.remove();
        }
      }
    }
  }

  @Override
  public CompletableFuture<ClusterEventService> open() {
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
    clusterCommunicator.<Collection<Subscription>, Void>addSubscriber(GOSSIP_MESSAGE_SUBJECT, SERIALIZER::decode, subscriptions -> {
      update(subscriptions);
      return null;
    }, SERIALIZER::encode, gossipExecutor);
    LOGGER.info("Started");
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    if (gossipExecutor != null) {
      gossipExecutor.shutdown();
    }
    LOGGER.info("Stopped");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }

  /**
   * Subscriber iterator that iterates subscribers in a loop.
   */
  private class SubscriberIterator implements Iterator<NodeId> {
    private final AtomicInteger counter = new AtomicInteger();
    private final NodeId[] subscribers;
    private final int length;

    SubscriberIterator(Collection<? extends NodeId> subscribers) {
      this.length = subscribers.size();
      this.subscribers = subscribers.toArray(new NodeId[length]);
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public NodeId next() {
      return subscribers[counter.incrementAndGet() % length];
    }
  }
}
