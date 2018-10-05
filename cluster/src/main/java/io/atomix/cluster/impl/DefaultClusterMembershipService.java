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
import com.google.common.collect.Lists;
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
import io.atomix.utils.Version;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-metadata";

  private static final Serializer SERIALIZER = Serializer.using(
      Namespace.builder()
          .register(Namespaces.BASIC)
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
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
      Version version,
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
        localMember.properties(),
        version);
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
    if (!members.containsKey(member.id())) {
      sendHeartbeat(member);
    }
  }

  /**
   * Handles a node leave event.
   */
  private void handleLeaveEvent(Node node) {
    members.compute(MemberId.from(node.id().id()), (id, member) -> member == null || !member.isActive() ? null : member);
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats() {
    checkMetadata();
    Stream<StatefulMember> clusterMembers = members.values().stream()
        .filter(member -> !member.id().equals(localMember.id()));

    Stream<StatefulMember> providerMembers = discoveryService.getNodes().stream()
        .filter(node -> !members.containsKey(MemberId.from(node.id().id())))
        .map(node -> new StatefulMember(MemberId.from(node.id().id()), node.address()));

    return Futures.allOf(Stream.concat(clusterMembers, providerMembers)
        .map(member -> {
          LOGGER.trace("{} - Sending heartbeat: {}", localMember.id(), member);
          return sendHeartbeat(member).exceptionally(v -> null);
        }).collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Checks the local member metadata for changes.
   */
  private void checkMetadata() {
    if (!localMember.properties().equals(localProperties)) {
      synchronized (this) {
        if (!localMember.properties().equals(localProperties)) {
          localProperties = localMember.properties();
          localMember.incrementTimestamp();
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.METADATA_CHANGED, localMember));
        }
      }
    }
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(StatefulMember member) {
    return bootstrapService.getMessagingService().sendAndReceive(member.address(), HEARTBEAT_MESSAGE, SERIALIZER.encode(localMember))
        .whenCompleteAsync((response, error) -> {
          if (error == null) {
            Collection<StatefulMember> remoteMembers = SERIALIZER.decode(response);
            for (StatefulMember remoteMember : remoteMembers) {
              if (remoteMember.id().equals(localMember.id()) || !remoteMember.isReachable()) {
                continue;
              }

              StatefulMember localMember = members.get(remoteMember.id());
              if (localMember == null) {
                members.put(remoteMember.id(), remoteMember);
                post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, remoteMember));
              } else if (localMember.getTimestamp() < remoteMember.getTimestamp()) {
                members.put(remoteMember.id(), remoteMember);
                post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.METADATA_CHANGED, localMember));
              }
            }
          } else {
            LOGGER.debug("{} - Sending heartbeat to {} failed", localMember.id(), member, error);
            if (member.isReachable()) {
              member.setReachable(false);
              post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, member));
            }

            PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(member.id(), n -> new PhiAccrualFailureDetector());
            double phi = failureDetector.phi();
            if (phi >= config.getReachabilityThreshold()
                || (phi == 0.0 && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getReachabilityTimeout().toMillis())) {
              if (members.remove(member.id()) != null) {
                failureDetectors.remove(member.id());
                post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, member));
              }
            }
          }
        }, heartbeatScheduler).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Address address, byte[] message) {
    StatefulMember remoteMember = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat: {}", localMember.id(), remoteMember);
    failureDetectors.computeIfAbsent(remoteMember.id(), n -> new PhiAccrualFailureDetector()).report();
    StatefulMember member = members.get(remoteMember.id());

    if (member == null) {
      remoteMember.setActive(true);
      remoteMember.setReachable(true);
      members.put(remoteMember.id(), remoteMember);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, remoteMember));
    } else {
      // If the member's metadata changed, overwrite the local member. We do this instead of a version check since
      // a heartbeat from the source member always takes precedence over a more recent version.
      if (!Objects.equals(member.version(), remoteMember.version())
          || !Objects.equals(member.zone(), remoteMember.zone())
          || !Objects.equals(member.rack(), remoteMember.rack())
          || !Objects.equals(member.host(), remoteMember.host())
          || !Objects.equals(member.properties(), remoteMember.properties())) {
        members.put(remoteMember.id(), remoteMember);
        if (member.isActive()) {
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.METADATA_CHANGED, member));
        }
        member = remoteMember;
      }

      // If the local member is not active, set it to active and trigger a MEMBER_ADDED event.
      if (!member.isActive()) {
        member.setActive(true);
        member.setReachable(true);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, member));
      }
      // If the member has been marked unreachable, mark it as reachable and trigger a REACHABILITY_CHANGED event.
      else if (!member.isReachable()) {
        member.setReachable(true);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, member));
      }
    }
    return SERIALIZER.encode(Lists.newArrayList(members.values()));
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
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, localMember));
        bootstrapService.getMessagingService().registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatScheduler);
        heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, config.getBroadcastInterval().toMillis(), TimeUnit.MILLISECONDS);
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
            bootstrapService.getMessagingService().unregisterHandler(HEARTBEAT_MESSAGE);
            LOGGER.info("Stopped");
          });
    }
    return CompletableFuture.completedFuture(null);
  }
}
