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
package io.atomix.cluster.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ManagedClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MembershipConfig;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.ManagedNodeDiscoveryService;
import io.atomix.cluster.discovery.NodeDiscoveryEvent;
import io.atomix.cluster.discovery.NodeDiscoveryEventListener;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Default cluster implementation.
 */
public class DefaultClusterMembershipService
    extends AbstractListenerManager<ClusterMembershipEvent, ClusterMembershipEventListener>
    implements ManagedClusterMembershipService {

  private static final Logger LOGGER = getLogger(DefaultClusterMembershipService.class);

  private static final String METADATA_BROADCAST = "atomix-cluster-metadata";

  private static final Serializer SERIALIZER = Serializer.using(
      Namespace.builder()
          .register(Namespaces.BASIC)
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
          .register(ClusterHeartbeat.class)
          .register(StatefulMember.class)
          .register(new AddressSerializer(), Address.class)
          .build("ClusterMembershipService"));

  private final MembershipConfig config;
  private final ManagedNodeDiscoveryService discoveryService;
  private final BootstrapService bootstrapService;

  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulMember localMember;
  private volatile Properties localProperties = new Properties();
  private final Map<MemberId, StatefulMember> members = Maps.newConcurrentMap();
  private final Map<MemberId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final NodeDiscoveryEventListener discoveryEventListener = this::handleDiscoveryEvent;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService eventExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-events", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public DefaultClusterMembershipService(
      Member localMember,
      ManagedNodeDiscoveryService discoveryService,
      BootstrapService bootstrapService,
      MembershipConfig config) {
    this.discoveryService = checkNotNull(discoveryService, "discoveryService cannot be null");
    this.bootstrapService = checkNotNull(bootstrapService, "bootstrapService cannot be null");
    this.config = checkNotNull(config);
    this.localMember = new StatefulMember(
        localMember.id(),
        localMember.address(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.properties());
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return ImmutableSet.copyOf(members.values());
  }

  @Override
  public Member getMember(MemberId memberId) {
    return members.get(memberId);
  }

  @Override
  protected void post(ClusterMembershipEvent event) {
    eventExecutor.execute(() -> super.post(event));
  }

  /**
   * Handles a member location event.
   *
   * @param event the member location event
   */
  private void handleDiscoveryEvent(NodeDiscoveryEvent event) {
    switch (event.type()) {
      case JOIN:
        handleJoinEvent(event.subject());
        break;
      case LEAVE:
        handleLeaveEvent(event.subject());
        break;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Handles a node join event.
   */
  private void handleJoinEvent(Node node) {
    StatefulMember member = new StatefulMember(MemberId.from(node.id().id()), node.address());
    member.setActive(true);
    member.setReachable(true);
    if (members.putIfAbsent(member.id(), member) == null) {
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, member));
    }
  }

  /**
   * Handles a node leave event.
   */
  private void handleLeaveEvent(Node node) {
    StatefulMember member = members.remove(MemberId.from(node.id().id()));
    if (member != null) {
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, member));
    }
  }

  /**
   * Checks the local member metadata for changes.
   */
  private void checkMetadata() {
    Member localMember = getLocalMember();
    if (!localMember.properties().equals(localProperties)) {
      synchronized (this) {
        if (!localMember.properties().equals(localProperties)) {
          localProperties = localMember.properties();
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.METADATA_CHANGED, localMember));
          broadcastMetadata();
        }
      }
    }
  }

  /**
   * Broadcasts a local member metadata change to all peers.
   */
  private void broadcastMetadata() {
    checkMetadata();
    members.values().stream()
        .filter(member -> !member.id().equals(localMember.id()))
        .forEach(this::broadcastMetadata);
    detectFailures();
  }

  /**
   * Broadcast the local member metadata to the given peer.
   *
   * @param member the member to which to broadcast the metadata
   */
  private void broadcastMetadata(StatefulMember member) {
    LOGGER.trace("{} - Sending heartbeat to {}", localMember.id(), member);
    bootstrapService.getMessagingService().sendAsync(member.address(), METADATA_BROADCAST, SERIALIZER.encode(localMember))
        .whenComplete((result, error) -> {
          if (error != null) {
            LOGGER.debug("{} - Failed to send heartbeat to {}", localMember.id(), member, error);
          } else {
            LOGGER.trace("{} - Successfully sent heartbeat to {}", localMember.id(), member);
          }
        });
  }

  /**
   * Handles a metadata broadcast from the given address.
   *
   * @param address the address from which the metadata was broadcast
   * @param message the broadcast message
   */
  private void handleMetadata(Address address, byte[] message) {
    StatefulMember remoteMember = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat from {}", localMember.id(), remoteMember);
    StatefulMember localMember = members.get(remoteMember.id());
    if (localMember != null) {
      if (!localMember.isReachable()) {
        LOGGER.info("{} - Member reachable: {}", localMember.id(), localMember);
        localMember.setReachable(true);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, localMember));
      }

      if (!localMember.properties().equals(remoteMember.properties())) {
        LOGGER.info("{} - Member updated: {}", remoteMember.id(), remoteMember);
        members.put(remoteMember.id(), remoteMember);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.METADATA_CHANGED, remoteMember));
      }
      failureDetectors.computeIfAbsent(localMember.id(), id -> new PhiAccrualFailureDetector()).report();
    }
  }

  /**
   * Checks for heartbeat failures.
   */
  private void detectFailures() {
    members.values().stream()
        .filter(member -> !member.id().equals(localMember.id()))
        .forEach(this::detectFailure);
  }

  /**
   * Checks the given member for a heartbeat failure.
   *
   * @param member the member to check
   */
  private void detectFailure(StatefulMember member) {
    PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(member.id(), n -> new PhiAccrualFailureDetector());
    double phi = failureDetector.phi();
    if (phi >= config.getReachabilityThreshold()
        || (phi == 0.0 && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getReachabilityTimeout().toMillis())) {
      if (member.isReachable()) {
        LOGGER.info("{} - Member unreachable: {}", localMember.id(), member);
        member.setReachable(false);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, member));
      }
    }
  }

  @Override
  public CompletableFuture<ClusterMembershipService> start() {
    if (started.compareAndSet(false, true)) {
      discoveryService.addListener(discoveryEventListener);
      return discoveryService.start().thenRun(() -> {
        LOGGER.info("{} - Member activated: {}", localMember.id(), localMember);
        localMember.setActive(true);
        localMember.setReachable(true);
        members.put(localMember.id(), localMember);
        bootstrapService.getMessagingService().registerHandler(METADATA_BROADCAST, this::handleMetadata, heartbeatScheduler);
        heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(
            this::broadcastMetadata, 0, config.getBroadcastInterval().toMillis(), TimeUnit.MILLISECONDS);
      }).thenApply(v -> {
        LOGGER.info("Started");
        return this;
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      return discoveryService.stop()
          .thenRun(() -> {
            discoveryService.removeListener(discoveryEventListener);
            heartbeatFuture.cancel(true);
            heartbeatScheduler.shutdownNow();
            eventExecutor.shutdownNow();
            LOGGER.info("{} - Member deactivated: {}", localMember.id(), localMember);
            localMember.setActive(false);
            localMember.setReachable(false);
            members.clear();
            bootstrapService.getMessagingService().unregisterHandler(METADATA_BROADCAST);
            LOGGER.info("Stopped");
          });
    }
    return CompletableFuture.completedFuture(null);
  }
}
