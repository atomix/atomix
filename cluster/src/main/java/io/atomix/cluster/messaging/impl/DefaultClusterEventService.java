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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ManagedClusterEventService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.Subscription;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
public class DefaultClusterEventService implements ManagedClusterEventService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterEventService.class);

  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(MemberId.class)
      .register(LogicalTimestamp.class)
      .register(WallClockTimestamp.class)
      .register(InternalSubscriptionInfo.class)
      .register(InternalMessage.class)
      .register(InternalMessage.Type.class)
      .build());

  private static final String GOSSIP_MESSAGE_SUBJECT = "ClusterEventingService-update";

  private static final long GOSSIP_INTERVAL_MILLIS = 1000;
  private static final long TOMBSTONE_EXPIRATION_MILLIS = 1000 * 60;

  private final ClusterMembershipService membershipService;
  private final MessagingService messagingService;
  private final MemberId localMemberId;
  private final AtomicLong logicalTime = new AtomicLong();
  private ScheduledExecutorService gossipExecutor;
  private final Map<MemberId, Long> updateTimes = Maps.newConcurrentMap();
  private final Map<String, InternalTopic> topics = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultClusterEventService(ClusterMembershipService membershipService, MessagingService messagingService) {
    this.membershipService = membershipService;
    this.messagingService = messagingService;
    this.localMemberId = membershipService.getLocalMember().id();
  }

  @Override
  public <M> void broadcast(String topic, M message, Function<M, byte[]> encoder) {
    byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.ALL, encoder.apply(message)));
    getSubscriberNodes(topic).forEach(memberId -> {
      Member member = membershipService.getMember(memberId);
      if (member != null && member.isReachable()) {
        messagingService.sendAsync(member.address(), topic, payload);
      }
    });
  }

  @Override
  public <M> CompletableFuture<Void> unicast(String topic, M message, Function<M, byte[]> encoder) {
    MemberId memberId = getNextMemberId(topic);
    if (memberId != null) {
      Member member = membershipService.getMember(memberId);
      if (member != null && member.isReachable()) {
        byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.DIRECT, encoder.apply(message)));
        return messagingService.sendAsync(member.address(), topic, payload);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<R> send(String topic, M message, Function<M, byte[]> encoder, Function<byte[], R> decoder, Duration timeout) {
    MemberId memberId = getNextMemberId(topic);
    if (memberId != null) {
      Member member = membershipService.getMember(memberId);
      if (member != null && member.isReachable()) {
        byte[] payload = SERIALIZER.encode(new InternalMessage(InternalMessage.Type.DIRECT, encoder.apply(message)));
        return messagingService.sendAndReceive(member.address(), topic, payload, timeout).thenApply(decoder);
      }
    }
    return Futures.exceptionalFuture(new MessagingException.NoRemoteHandler());
  }

  /**
   * Returns a collection of nodes that subscribe to the given topic.
   *
   * @param topicName the topic for which to return the collection of subscriber nodes
   * @return the collection of subscribers for the given topic
   */
  private Stream<MemberId> getSubscriberNodes(String topicName) {
    InternalTopic topic = topics.get(topicName);
    if (topic == null) {
      return Stream.empty();
    }
    return topic.remoteSubscriptions().stream()
        .filter(s -> !s.isTombstone())
        .map(s -> s.memberId())
        .distinct();
  }

  /**
   * Returns the next node ID for the given message topic.
   *
   * @param topicName the topic for which to return the next node ID
   * @return the next node ID for the given message topic
   */
  private MemberId getNextMemberId(String topicName) {
    InternalTopic topic = topics.get(topicName);
    if (topic == null) {
      return null;
    }

    TopicIterator iterator = topic.iterator();
    if (iterator.hasNext()) {
      return iterator.next().memberId();
    }
    return null;
  }

  @Override
  public <M, R> CompletableFuture<Subscription> subscribe(
      String topic, Function<byte[], M> decoder, Function<M, R> handler, Function<R, byte[]> encoder, Executor executor) {
    return topics.computeIfAbsent(topic, t -> new InternalTopic(topic))
        .subscribe(decoder, handler, encoder, executor);
  }

  @Override
  public <M, R> CompletableFuture<Subscription> subscribe(
      String topic, Function<byte[], M> decoder, Function<M, CompletableFuture<R>> handler, Function<R, byte[]> encoder) {
    return topics.computeIfAbsent(topic, t -> new InternalTopic(topic))
        .subscribe(decoder, handler, encoder);
  }

  @Override
  public <M> CompletableFuture<Subscription> subscribe(
      String topic, Function<byte[], M> decoder, Consumer<M> handler, Executor executor) {
    return topics.computeIfAbsent(topic, t -> new InternalTopic(topic))
        .subscribe(decoder, handler, executor);
  }

  @Override
  public List<Subscription> getSubscriptions(String topicName) {
    InternalTopic topic = topics.get(topicName);
    if (topic == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(topic.localSubscriber().subscriptions());
  }

  /**
   * Handles a collection of subscription updates received via the gossip protocol.
   *
   * @param subscriptions a collection of subscriptions provided by the sender
   */
  private void update(Collection<InternalSubscriptionInfo> subscriptions) {
    for (InternalSubscriptionInfo subscription : subscriptions) {
      InternalTopic topic = topics.computeIfAbsent(subscription.topic, InternalTopic::new);
      InternalSubscriptionInfo matchingSubscription = topic.remoteSubscriptions().stream()
          .filter(s -> s.memberId().equals(subscription.memberId()) && s.logicalTimestamp().equals(subscription.logicalTimestamp()))
          .findFirst()
          .orElse(null);
      if (matchingSubscription == null) {
        topic.addRemoteSubscription(subscription);
      } else if (subscription.isTombstone()) {
        topic.removeRemoteSubscription(subscription);
      }
    }
  }

  /**
   * Sends a gossip message to an active peer.
   */
  private void gossip() {
    List<Member> members = membershipService.getMembers()
        .stream()
        .filter(node -> !localMemberId.equals(node.id()))
        .filter(node -> node.isReachable())
        .collect(Collectors.toList());

    if (!members.isEmpty()) {
      Collections.shuffle(members);
      Member member = members.get(0);
      updateNode(member);
    }
  }

  /**
   * Updates all active peers with a given subscription.
   */
  private CompletableFuture<Void> updateNodes() {
    List<CompletableFuture<Void>> futures = membershipService.getMembers()
        .stream()
        .filter(node -> !localMemberId.equals(node.id()))
        .map(this::updateNode)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Sends an update to the given node.
   *
   * @param member the node to which to send the update
   */
  private CompletableFuture<Void> updateNode(Member member) {
    long updateTime = System.currentTimeMillis();
    long lastUpdateTime = updateTimes.getOrDefault(member.id(), 0L);

    Collection<InternalSubscriptionInfo> subscriptions = topics.values()
        .stream()
        .flatMap(t -> t.remoteSubscriptions().stream().filter(subscriber -> subscriber.timestamp().unixTimestamp() >= lastUpdateTime))
        .collect(Collectors.toList());

    CompletableFuture<Void> future = new CompletableFuture<>();
    messagingService.sendAndReceive(member.address(), GOSSIP_MESSAGE_SUBJECT, SERIALIZER.encode(subscriptions))
        .whenComplete((result, error) -> {
          if (error == null) {
            updateTimes.put(member.id(), updateTime);
          }
          future.complete(null);
        });
    return future;
  }

  /**
   * Purges tombstones from the subscription list.
   */
  private void purgeTombstones() {
    long minTombstoneTime = membershipService.getMembers()
        .stream()
        .map(node -> updateTimes.getOrDefault(node.id(), 0L))
        .reduce(Math::min)
        .orElse(0L);
    for (InternalTopic topic : topics.values()) {
      topic.purgeTombstones(minTombstoneTime);
    }
  }

  @Override
  public CompletableFuture<ClusterEventService> start() {
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
      messagingService.registerHandler(GOSSIP_MESSAGE_SUBJECT, (address, payload) -> {
        update(SERIALIZER.decode(payload));
        return new byte[0];
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
   * Internal topic.
   */
  private class InternalTopic {
    private final String topic;
    private final InternalSubscriber subscribers = new InternalSubscriber();
    private final List<InternalSubscriptionInfo> subscriptions = Lists.newCopyOnWriteArrayList();
    private TopicIterator iterator;

    InternalTopic(String topic) {
      this.topic = topic;
    }

    /**
     * Returns the local subscriber for the topic.
     *
     * @return the local subscriber for the topic
     */
    InternalSubscriber localSubscriber() {
      return subscribers;
    }

    /**
     * Returns the list of remote subscriptions for the topic.
     *
     * @return the list of remote subscriptions for the topic
     */
    List<InternalSubscriptionInfo> remoteSubscriptions() {
      return subscriptions;
    }

    /**
     * Returns the topic subscription iterator.
     *
     * @return the topic subscription iterator
     */
    TopicIterator iterator() {
      return iterator;
    }

    /**
     * Subscribes to messages from the topic.
     */
    <M, R> CompletableFuture<Subscription> subscribe(
        Function<byte[], M> decoder, Function<M, R> handler, Function<R, byte[]> encoder, Executor executor) {
      return addLocalSubscription(new InternalSubscription(this, payload -> {
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

    /**
     * Subscribes to messages from the topic.
     */
    <M, R> CompletableFuture<Subscription> subscribe(
        Function<byte[], M> decoder, Function<M, CompletableFuture<R>> handler, Function<R, byte[]> encoder) {
      return addLocalSubscription(new InternalSubscription(this, payload -> {
        return handler.apply(decoder.apply(payload)).thenApply(encoder);
      }));
    }

    /**
     * Subscribes to messages from the topic.
     */
    <M> CompletableFuture<Subscription> subscribe(
        Function<byte[], M> decoder, Consumer<M> handler, Executor executor) {
      return addLocalSubscription(new InternalSubscription(this, payload -> {
        executor.execute(() -> {
          try {
            handler.accept(decoder.apply(payload));
          } catch (Exception e) {
          }
        });
        return CompletableFuture.completedFuture(null);
      }));
    }

    /**
     * Registers the node as a subscriber for the given topic.
     *
     * @param subscription the subscription to register
     */
    private synchronized CompletableFuture<Subscription> addLocalSubscription(InternalSubscription subscription) {
      subscribers.add(subscription);
      subscriptions.add(subscription.metadata);
      iterator = new TopicIterator(subscriptions);
      messagingService.registerHandler(subscription.topic(), subscribers);
      return updateNodes().thenApply(v -> subscription);
    }

    /**
     * Unregisters the node as a subscriber for the given topic.
     *
     * @param subscription the subscription to unregister
     */
    private synchronized CompletableFuture<Void> removeLocalSubscription(InternalSubscription subscription) {
      subscribers.remove(subscription);
      subscriptions.remove(subscription.metadata);
      subscriptions.add(subscription.metadata.asTombstone());
      iterator = new TopicIterator(subscriptions);
      if (subscriptions.stream().filter(s -> s.isTombstone()).count() == 0) {
        messagingService.unregisterHandler(subscription.topic());
      }
      return updateNodes();
    }

    /**
     * Adds a subscription to the topic.
     *
     * @param subscription the subscription to add
     */
    synchronized void addRemoteSubscription(InternalSubscriptionInfo subscription) {
      subscriptions.add(subscription);
      iterator = new TopicIterator(subscriptions);
    }

    /**
     * Updates a subscription to the topic.
     *
     * @param subscription the subscription to update
     */
    synchronized void removeRemoteSubscription(InternalSubscriptionInfo subscription) {
      subscriptions.remove(subscription);
      subscriptions.add(subscription);
      iterator = new TopicIterator(subscriptions);
    }

    /**
     * Purges tombstones from the topic.
     *
     * @param minTombstoneTime the time before which tombstones can be removed
     */
    synchronized void purgeTombstones(long minTombstoneTime) {
      int startSize = subscriptions.size();
      subscriptions.removeIf(subscription -> {
        return subscription.isTombstone() && subscription.timestamp().unixTimestamp() < minTombstoneTime;
      });
      if (subscriptions.size() != startSize) {
        iterator = new TopicIterator(subscriptions);
      }
    }
  }

  /**
   * Subscriber iterator that iterates subscribers in a loop.
   */
  private static class TopicIterator implements Iterator<InternalSubscriptionInfo> {
    private final AtomicInteger counter = new AtomicInteger();
    private final InternalSubscriptionInfo[] subscribers;

    TopicIterator(List<InternalSubscriptionInfo> subscribers) {
      List<InternalSubscriptionInfo> filteredSubscribers = subscribers.stream()
          .filter(s -> !s.isTombstone())
          .collect(Collectors.toList());
      Collections.reverse(filteredSubscribers);
      this.subscribers = filteredSubscribers.toArray(new InternalSubscriptionInfo[filteredSubscribers.size()]);
    }

    @Override
    public boolean hasNext() {
      return subscribers.length > 0;
    }

    @Override
    public InternalSubscriptionInfo next() {
      return subscribers[Math.abs(counter.incrementAndGet() % subscribers.length)];
    }
  }

  /**
   * Internal subscriber.
   */
  private static class InternalSubscriber implements BiFunction<Address, byte[], CompletableFuture<byte[]>> {
    private final AtomicInteger counter = new AtomicInteger();
    private InternalSubscription[] subscriptions = new InternalSubscription[0];

    /**
     * Returns a list of subscriptions within the subscriber.
     *
     * @return a list of subscriptions
     */
    List<InternalSubscription> subscriptions() {
      return ImmutableList.copyOf(subscriptions);
    }

    /**
     * Returns the next subscription.
     *
     * @return the next subscription
     */
    private InternalSubscription next() {
      InternalSubscription[] subscriptions = this.subscriptions;
      return subscriptions[counter.incrementAndGet() % subscriptions.length];
    }

    @Override
    public CompletableFuture<byte[]> apply(Address address, byte[] payload) {
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

    /**
     * Adds a local subscription.
     *
     * @param subscription the subscription to add
     */
    void add(InternalSubscription subscription) {
      List<InternalSubscription> subscriptions = new ArrayList<>(this.subscriptions.length + 1);
      subscriptions.addAll(Arrays.asList(this.subscriptions));
      subscriptions.add(subscription);
      this.subscriptions = subscriptions.toArray(new InternalSubscription[subscriptions.size()]);
    }

    /**
     * Removes a local subscription.
     *
     * @param subscription the subscription to remove
     */
    void remove(InternalSubscription subscription) {
      List<InternalSubscription> subscriptions = Lists.newArrayList(this.subscriptions);
      subscriptions.remove(subscription);
      this.subscriptions = subscriptions.toArray(new InternalSubscription[subscriptions.size()]);
    }
  }

  /**
   * Internal subscription.
   */
  private class InternalSubscription implements Subscription {
    private final InternalTopic topic;
    private final InternalSubscriptionInfo metadata;
    private final Function<byte[], CompletableFuture<byte[]>> callback;

    InternalSubscription(InternalTopic topic, Function<byte[], CompletableFuture<byte[]>> callback) {
      this.topic = topic;
      this.metadata = new InternalSubscriptionInfo(localMemberId, topic.topic, new LogicalTimestamp(logicalTime.incrementAndGet()));
      this.callback = callback;
    }

    @Override
    public String topic() {
      return metadata.topic();
    }

    @Override
    public CompletableFuture<Void> close() {
      return topic.removeLocalSubscription(this);
    }
  }

  /**
   * Subscription metadata.
   */
  private static class InternalSubscriptionInfo {
    private final MemberId memberId;
    private final String topic;
    private final LogicalTimestamp logicalTimestamp;
    private final boolean tombstone;
    private final WallClockTimestamp timestamp = new WallClockTimestamp();

    InternalSubscriptionInfo(MemberId memberId, String topic, LogicalTimestamp logicalTimestamp) {
      this(memberId, topic, logicalTimestamp, false);
    }

    InternalSubscriptionInfo(MemberId memberId, String topic, LogicalTimestamp logicalTimestamp, boolean tombstone) {
      this.memberId = memberId;
      this.topic = topic;
      this.logicalTimestamp = logicalTimestamp;
      this.tombstone = tombstone;
    }

    /**
     * Returns the node to which the subscription belongs.
     *
     * @return the node to which the subscription belongs
     */
    MemberId memberId() {
      return memberId;
    }

    /**
     * Returns the topic name.
     *
     * @return the topic name
     */
    String topic() {
      return topic;
    }

    /**
     * Returns the logical time at which the subscription was created.
     *
     * @return the logical time at which the subscription was created
     */
    LogicalTimestamp logicalTimestamp() {
      return logicalTimestamp;
    }

    /**
     * Returns the wall clock time at which the subscription was created.
     *
     * @return the wall clock time at which the subscription was created
     */
    WallClockTimestamp timestamp() {
      return timestamp;
    }

    /**
     * Returns a boolean indicating whether the subscription is a tombstone.
     *
     * @return indicates whether the subscription is a tombstone
     */
    boolean isTombstone() {
      return tombstone;
    }

    /**
     * Returns a new subscription as a tombstone.
     *
     * @return the subscription as a tombstone
     */
    InternalSubscriptionInfo asTombstone() {
      return new InternalSubscriptionInfo(memberId, topic, logicalTimestamp, true);
    }
  }
}
