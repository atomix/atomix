/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.protocol;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.NodeDiscoveryEvent;
import io.atomix.cluster.discovery.NodeDiscoveryEventListener;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.cluster.impl.AddressSerializer;
import io.atomix.cluster.impl.PhiAccrualFailureDetector;
import io.atomix.utils.Version;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Gossip based group membership protocol.
 */
public class PhiMembershipProtocol
    extends AbstractListenerManager<GroupMembershipEvent, GroupMembershipEventListener>
    implements GroupMembershipProtocol {

  public static final Type TYPE = new Type();

  /**
   * Creates a new bootstrap provider builder.
   *
   * @return a new bootstrap provider builder
   */
  public static PhiMembershipProtocolBuilder builder() {
    return new PhiMembershipProtocolBuilder();
  }

  /**
   * Bootstrap member location provider type.
   */
  public static class Type implements GroupMembershipProtocol.Type<PhiMembershipProtocolConfig> {
    private static final String NAME = "phi";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PhiMembershipProtocolConfig newConfig() {
      return new PhiMembershipProtocolConfig();
    }

    @Override
    public GroupMembershipProtocol newProtocol(PhiMembershipProtocolConfig config) {
      return new PhiMembershipProtocol(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PhiMembershipProtocol.class);

  private final PhiMembershipProtocolConfig config;

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-membership";

  private static final Serializer SERIALIZER = Serializer.using(
      Namespace.builder()
          .register(Namespaces.BASIC)
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
          .register(GossipMember.class)
          .register(new AddressSerializer(), Address.class)
          .build("ClusterMembershipService"));

  private volatile NodeDiscoveryService discoveryService;
  private volatile BootstrapService bootstrapService;

  private final AtomicBoolean started = new AtomicBoolean();
  private volatile GossipMember localMember;
  private volatile Properties localProperties = new Properties();
  private final Map<MemberId, GossipMember> members = Maps.newConcurrentMap();
  private final Map<MemberId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final NodeDiscoveryEventListener discoveryEventListener = this::handleDiscoveryEvent;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService eventExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-events", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public PhiMembershipProtocol(PhiMembershipProtocolConfig config) {
    this.config = config;
  }

  @Override
  public GroupMembershipProtocolConfig config() {
    return config;
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
  protected void post(GroupMembershipEvent event) {
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
    GossipMember member = new GossipMember(MemberId.from(node.id().id()), node.address());
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
    Stream<GossipMember> clusterMembers = members.values().stream()
        .filter(member -> !member.id().equals(localMember.id()));

    Stream<GossipMember> providerMembers = discoveryService.getNodes().stream()
        .filter(node -> !members.containsKey(MemberId.from(node.id().id())))
        .map(node -> new GossipMember(MemberId.from(node.id().id()), node.address()));

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
          post(new GroupMembershipEvent(GroupMembershipEvent.Type.METADATA_CHANGED, localMember));
        }
      }
    }
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(GossipMember member) {
    return bootstrapService.getMessagingService().sendAndReceive(member.address(), HEARTBEAT_MESSAGE, SERIALIZER.encode(localMember))
        .whenCompleteAsync((response, error) -> {
          if (error == null) {
            Collection<GossipMember> remoteMembers = SERIALIZER.decode(response);
            for (GossipMember remoteMember : remoteMembers) {
              if (remoteMember.id().equals(localMember.id()) || !remoteMember.isReachable()) {
                continue;
              }

              GossipMember localMember = members.get(remoteMember.id());
              if (localMember == null) {
                members.put(remoteMember.id(), remoteMember);
                post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, remoteMember));
              } else if (localMember.getTimestamp() < remoteMember.getTimestamp()) {
                members.put(remoteMember.id(), remoteMember);
                post(new GroupMembershipEvent(GroupMembershipEvent.Type.METADATA_CHANGED, localMember));
              }
            }
          } else {
            LOGGER.debug("{} - Sending heartbeat to {} failed", localMember.id(), member, error);
            if (member.isReachable()) {
              member.setReachable(false);
              post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, member));
            }

            PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(member.id(), n -> new PhiAccrualFailureDetector());
            double phi = failureDetector.phi();
            if (phi >= config.getFailureThreshold()
                || (phi == 0.0 && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getFailureTimeout().toMillis())) {
              if (members.remove(member.id()) != null) {
                failureDetectors.remove(member.id());
                post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_REMOVED, member));
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
    GossipMember remoteMember = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat: {}", localMember.id(), remoteMember);
    failureDetectors.computeIfAbsent(remoteMember.id(), n -> new PhiAccrualFailureDetector()).report();
    GossipMember member = members.get(remoteMember.id());

    if (member == null) {
      remoteMember.setActive(true);
      remoteMember.setReachable(true);
      members.put(remoteMember.id(), remoteMember);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, remoteMember));
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
          post(new GroupMembershipEvent(GroupMembershipEvent.Type.METADATA_CHANGED, member));
        }
        member = remoteMember;
      }

      // If the local member is not active, set it to active and trigger a MEMBER_ADDED event.
      if (!member.isActive()) {
        member.setActive(true);
        member.setReachable(true);
        post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, member));
      }
      // If the member has been marked unreachable, mark it as reachable and trigger a REACHABILITY_CHANGED event.
      else if (!member.isReachable()) {
        member.setReachable(true);
        post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, member));
      }
    }
    return SERIALIZER.encode(Lists.newArrayList(members.values()));
  }

  @Override
  public CompletableFuture<Void> join(BootstrapService bootstrap, NodeDiscoveryService discovery, Member member) {
    if (started.compareAndSet(false, true)) {
      this.bootstrapService = bootstrap;
      this.discoveryService = discovery;
      this.localMember = new GossipMember(
          member.id(),
          member.address(),
          member.zone(),
          member.rack(),
          member.host(),
          member.properties(),
          member.version());
      discoveryService.addListener(discoveryEventListener);

      LOGGER.info("{} - Member activated: {}", localMember.id(), localMember);
      localMember.setActive(true);
      localMember.setReachable(true);
      members.put(localMember.id(), localMember);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, localMember));

      bootstrapService.getMessagingService().registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatScheduler);
      heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(
          this::sendHeartbeats, 0, config.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
      LOGGER.info("Started");
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave(Member member) {
    if (started.compareAndSet(true, false)) {
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
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Internal gossip based group member.
   */
  private static class GossipMember extends Member {
    private final Version version;
    private final AtomicLong timestamp = new AtomicLong();
    private volatile boolean active;
    private volatile boolean reachable;

    public GossipMember(MemberId id, Address address) {
      super(id, address);
      this.version = null;
      timestamp.set(0);
    }

    public GossipMember(
        MemberId id,
        Address address,
        String zone,
        String rack,
        String host,
        Properties properties,
        Version version) {
      super(id, address, zone, rack, host, properties);
      this.version = version;
      timestamp.set(1);
    }

    @Override
    public Version version() {
      return version;
    }

    /**
     * Returns the member logical timestamp.
     *
     * @return the member logical timestamp
     */
    public long getTimestamp() {
      return timestamp.get();
    }

    /**
     * Sets the member's logical timestamp.
     *
     * @param timestamp the member's logical timestamp
     */
    void setTimestamp(long timestamp) {
      this.timestamp.accumulateAndGet(timestamp, Math::max);
    }

    /**
     * Increments the member's timestamp.
     */
    void incrementTimestamp() {
      timestamp.incrementAndGet();
    }

    /**
     * Sets whether this member is an active member of the cluster.
     *
     * @param active whether this member is an active member of the cluster
     */
    void setActive(boolean active) {
      this.active = active;
    }

    /**
     * Sets whether this member is reachable.
     *
     * @param reachable whether this member is reachable
     */
    void setReachable(boolean reachable) {
      this.reachable = reachable;
    }

    @Override
    public boolean isActive() {
      return active;
    }

    @Override
    public boolean isReachable() {
      return reachable;
    }
  }
}
