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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Gossip based group membership protocol.
 */
public class HeartbeatMembershipProtocol
    extends AbstractListenerManager<GroupMembershipEvent, GroupMembershipEventListener>
    implements GroupMembershipProtocol {

  public static final Type TYPE = new Type();

  /**
   * Creates a new bootstrap provider builder.
   *
   * @return a new bootstrap provider builder
   */
  public static HeartbeatMembershipProtocolBuilder builder() {
    return new HeartbeatMembershipProtocolBuilder();
  }

  /**
   * Bootstrap member location provider type.
   */
  public static class Type implements GroupMembershipProtocol.Type<HeartbeatMembershipProtocolConfig> {
    private static final String NAME = "heartbeat";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public HeartbeatMembershipProtocolConfig newConfig() {
      return new HeartbeatMembershipProtocolConfig();
    }

    @Override
    public GroupMembershipProtocol newProtocol(HeartbeatMembershipProtocolConfig config) {
      return new HeartbeatMembershipProtocol(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatMembershipProtocol.class);

  private final HeartbeatMembershipProtocolConfig config;

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

  public HeartbeatMembershipProtocol(HeartbeatMembershipProtocolConfig config) {
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
      localProperties = new Properties();
      localProperties.putAll(localMember.properties());
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.METADATA_CHANGED, localMember));
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
              if (!remoteMember.id().equals(localMember.id())) {
                updateMember(remoteMember, remoteMember.id().equals(member.id()));
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
            if (phi >= config.getPhiFailureThreshold()
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
    updateMember(remoteMember, true);

    // Return only reachable members to avoid populating removed members on remote nodes from unreachable members.
    return SERIALIZER.encode(Lists.newArrayList(members.values()
        .stream()
        .filter(member -> member.isReachable())
        .collect(Collectors.toList())));
  }

  /**
   * Updates the state of the given member.
   *
   * @param remoteMember the member received from a remote node
   * @param direct whether this is a direct update
   */
  private void updateMember(GossipMember remoteMember, boolean direct) {
    GossipMember localMember = members.get(remoteMember.id());
    if (localMember == null) {
      remoteMember.setActive(true);
      remoteMember.setReachable(true);
      members.put(remoteMember.id(), remoteMember);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, remoteMember));
    } else if (!Objects.equals(localMember.version(), remoteMember.version())) {
      members.remove(localMember.id());
      localMember.setReachable(false);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, localMember));
      localMember.setActive(false);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_REMOVED, localMember));
      members.put(remoteMember.id(), remoteMember);
      remoteMember.setActive(true);
      remoteMember.setReachable(true);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.MEMBER_ADDED, remoteMember));
    } else if (!Objects.equals(localMember.properties(), remoteMember.properties())) {
      if (!localMember.isReachable()) {
        localMember.setReachable(true);
        post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, localMember));
      }
      localMember.properties().putAll(remoteMember.properties());
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.METADATA_CHANGED, localMember));
    } else if (!localMember.isReachable() && direct) {
      localMember.setReachable(true);
      localMember.setTerm(localMember.getTerm() + 1);
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, localMember));
    } else if (!localMember.isReachable() && remoteMember.getTerm() > localMember.getTerm()) {
      localMember.setReachable(true);
      localMember.setTerm(remoteMember.getTerm());
      post(new GroupMembershipEvent(GroupMembershipEvent.Type.REACHABILITY_CHANGED, localMember));
    }
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
          member.version(),
          System.currentTimeMillis());
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
    private final long timestamp;
    private volatile boolean active;
    private volatile boolean reachable;
    private volatile long term;

    GossipMember(MemberId id, Address address) {
      super(id, address);
      this.version = null;
      this.timestamp = 0;
    }

    GossipMember(
        MemberId id,
        Address address,
        String zone,
        String rack,
        String host,
        Properties properties,
        Version version,
        long timestamp) {
      super(id, address, zone, rack, host, properties);
      this.version = version;
      this.timestamp = timestamp;
    }

    @Override
    public Version version() {
      return version;
    }

    @Override
    public long timestamp() {
      return timestamp;
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

    /**
     * Returns the member term.
     *
     * @return the member term
     */
    long getTerm() {
      return term;
    }

    /**
     * Sets the member term.
     *
     * @param term the member term
     */
    void setTerm(long term) {
      this.term = term;
    }
  }
}
