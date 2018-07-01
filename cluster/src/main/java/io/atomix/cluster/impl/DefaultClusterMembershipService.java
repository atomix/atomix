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
import io.atomix.cluster.Member.State;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MemberLocationEvent;
import io.atomix.cluster.MemberLocationEventListener;
import io.atomix.cluster.ClusterMembershipProvider;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

  private static final String MEMBER_INFO = "atomix-cluster-member-info";
  private static final String METADATA_BROADCAST = "atomix-cluster-metadata";

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private static final Serializer SERIALIZER = Serializer.using(
      Namespace.builder()
          .register(Namespaces.BASIC)
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
          .register(MemberId.Type.class)
          .register(Member.State.class)
          .register(ClusterHeartbeat.class)
          .register(StatefulMember.class)
          .register(new AddressSerializer(), Address.class)
          .build("ClusterMembershipService"));

  private final BootstrapService bootstrap;
  private final ClusterMembershipProvider locationProvider;

  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulMember localMember;
  private volatile Map<String, String> localMetadata = Maps.newConcurrentMap();
  private final Map<MemberId, StatefulMember> members = Maps.newConcurrentMap();
  private final MemberLocationEventListener locationEventListener = this::handleLocationEvent;

  private final ScheduledExecutorService metadataScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private ScheduledFuture<?> metadataFuture;

  public DefaultClusterMembershipService(
      Member localMember,
      BootstrapService bootstrap,
      ClusterMembershipProvider locationProvider) {
    this.bootstrap = checkNotNull(bootstrap, "bootstrapService cannot be null");
    this.locationProvider = checkNotNull(locationProvider, "locationProvider cannot be null");
    this.localMember = new StatefulMember(
        localMember.id(),
        localMember.address(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.metadata());
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return ImmutableSet.copyOf(members.values()
        .stream()
        .filter(member -> member.getState() == State.ACTIVE)
        .collect(Collectors.toList()));
  }

  @Override
  public Member getMember(MemberId memberId) {
    Member member = members.get(memberId);
    return member != null && member.getState() == State.ACTIVE ? member : null;
  }

  /**
   * Bootstraps all members in the cluster.
   *
   * @return a future to be completed once all members have been bootstrapped
   */
  private CompletableFuture<Void> bootstrapMembers() {
    return locationProvider.join(bootstrap, localMember.address())
        .thenCompose(v -> CompletableFuture.allOf(locationProvider.getLocations().stream()
            .filter(location -> !location.equals(localMember.address()))
            .map(this::bootstrapMember)
            .toArray(CompletableFuture[]::new)));
  }

  /**
   * Requests member information from the given address.
   *
   * @param address the address from which to request member information
   * @return a future to be completed once member info has been received
   */
  private CompletableFuture<Void> bootstrapMember(Address address) {
    LOGGER.debug("{} - Bootstrapping member {}", localMember.id(), address);
    return bootstrap.getMessagingService().sendAndReceive(address, MEMBER_INFO, EMPTY_PAYLOAD)
        .thenAccept(response -> {
          StatefulMember member = SERIALIZER.decode(response);
          if (members.putIfAbsent(member.id(), member) == null) {
            LOGGER.info("{} - Member added: {}", localMember.id(), member);
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, member));
          }
        }).exceptionally(e -> {
          LOGGER.debug("{} - Failed to bootstrap member {}", localMember.id(), address, e);
          return null;
        });
  }

  /**
   * Handles a member info request.
   *
   * @param address the address from which the request was sent
   * @param payload the message payload
   * @return the serialized local member info
   */
  private byte[] handleMemberInfo(Address address, byte[] payload) {
    return SERIALIZER.encode(localMember);
  }

  /**
   * Handles a member location event.
   *
   * @param event the member location event
   */
  private void handleLocationEvent(MemberLocationEvent event) {
    switch (event.type()) {
      case JOIN:
        if (!event.address().equals(localMember.address())) {
          bootstrapMember(event.address());
        }
        break;
      case LEAVE:
        members.values()
            .stream()
            .filter(m -> m.address().equals(event.address()))
            .findFirst()
            .ifPresent(m -> {
              StatefulMember member = members.remove(m.id());
              if (member != null) {
                post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, member));
              }
            });
        break;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Checks the local member metadata for changes.
   */
  private void checkMetadata() {
    Member localMember = getLocalMember();
    if (!localMember.metadata().equals(localMetadata)) {
      synchronized (this) {
        if (!localMember.metadata().equals(localMetadata)) {
          localMetadata = localMember.metadata();
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_UPDATED, localMember));
          broadcastMetadata();
        }
      }
    }
  }

  /**
   * Broadcasts a local member metadata change to all peers.
   */
  private void broadcastMetadata() {
    members.values().stream()
        .filter(member -> !member.id().equals(localMember.id()))
        .forEach(this::broadcastMetadata);
  }

  /**
   * Broadcast the local member metadata to the given peer.
   *
   * @param member the member to which to broadcast the metadata
   */
  private void broadcastMetadata(StatefulMember member) {
    bootstrap.getMessagingService().sendAsync(member.address(), METADATA_BROADCAST, SERIALIZER.encode(localMember));
  }

  /**
   * Handles a metadata broadcast from the given address.
   *
   * @param address the address from which the metadata was broadcast
   * @param message the broadcast message
   */
  private void handleMetadata(Address address, byte[] message) {
    StatefulMember remoteMember = SERIALIZER.decode(message);
    StatefulMember localMember = members.get(remoteMember.id());
    if (localMember != null && !localMember.metadata().equals(remoteMember.metadata())) {
      LOGGER.info("{} - Member updated: {}", remoteMember.id(), remoteMember);
      members.put(remoteMember.id(), remoteMember);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_UPDATED, remoteMember));
    }
  }

  @Override
  public CompletableFuture<ClusterMembershipService> start() {
    if (started.compareAndSet(false, true)) {
      locationProvider.addListener(locationEventListener);
      LOGGER.info("{} - Member activated: {}", localMember.id(), localMember);
      localMember.setState(State.ACTIVE);
      members.put(localMember.id(), localMember);
      bootstrap.getMessagingService().registerHandler(MEMBER_INFO, this::handleMemberInfo, metadataScheduler);
      bootstrap.getMessagingService().registerHandler(METADATA_BROADCAST, this::handleMetadata, metadataScheduler);
      metadataFuture = metadataScheduler.scheduleAtFixedRate(() -> checkMetadata(), 0, 1, TimeUnit.SECONDS);
      return bootstrapMembers().thenApply(v -> {
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
      return locationProvider.leave(localMember.address())
          .thenRun(() -> {
            metadataScheduler.shutdownNow();
            LOGGER.info("{} - Member deactivated: {}", localMember.id(), localMember);
            localMember.setState(State.INACTIVE);
            members.clear();
            metadataFuture.cancel(true);
            bootstrap.getMessagingService().unregisterHandler(MEMBER_INFO);
            bootstrap.getMessagingService().unregisterHandler(METADATA_BROADCAST);
            LOGGER.info("Stopped");
          });
    }
    return CompletableFuture.completedFuture(null);
  }
}
