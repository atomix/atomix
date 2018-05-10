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
import com.google.common.collect.Sets;
import io.atomix.cluster.BootstrapMetadataService;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.GroupMembershipConfig;
import io.atomix.cluster.ManagedClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.Member.State;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.PersistentMetadataService;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-heartbeat";

  private static final Serializer SERIALIZER = Serializer.using(
      KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(MemberId.class)
          .register(Member.Type.class)
          .register(Member.State.class)
          .register(ClusterHeartbeat.class)
          .register(StatefulMember.class)
          .register(new DefaultPersistentMetadataService.AddressSerializer(), Address.class)
          .build("ClusterMembershipService"));

  private final MessagingService messagingService;
  private final BroadcastService broadcastService;
  private final BootstrapMetadataService bootstrapMetadataService;
  private final PersistentMetadataService persistentMetadataService;

  private final int heartbeatInterval;
  private final int phiFailureThreshold;
  private final int failureTimeout;

  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulMember localMember;
  private final Map<MemberId, StatefulMember> members = Maps.newConcurrentMap();
  private final Map<MemberId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final ClusterMetadataEventListener metadataEventListener = this::handleMetadataEvent;
  private final Consumer<byte[]> broadcastListener = this::handleBroadcastMessage;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public DefaultClusterMembershipService(
      Member localMember,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService,
      GroupMembershipConfig config) {
    this.bootstrapMetadataService = checkNotNull(bootstrapMetadataService, "bootstrapMetadataService cannot be null");
    this.persistentMetadataService = checkNotNull(persistentMetadataService, "coreMetadataService cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.broadcastService = checkNotNull(broadcastService, "broadcastService cannot be null");
    this.localMember = new StatefulMember(
        localMember.id(),
        localMember.type(),
        localMember.address(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.metadata());
    this.heartbeatInterval = config.getHeartbeatInterval();
    this.phiFailureThreshold = config.getPhiFailureThreshold();
    this.failureTimeout = config.getFailureTimeout();
  }

  @Override
  public Member getLocalMember() {
    return localMember;
  }

  @Override
  public Set<Member> getMembers() {
    return ImmutableSet.copyOf(members.values()
        .stream()
        .filter(node -> node.type() == Member.Type.PERSISTENT || node.getState() == State.ACTIVE)
        .collect(Collectors.toList()));
  }

  @Override
  public Member getMember(MemberId memberId) {
    Member member = members.get(memberId);
    return member != null && (member.type() == Member.Type.PERSISTENT || member.getState() == State.ACTIVE) ? member : null;
  }

  /**
   * Broadcasts this node's identity.
   */
  private void broadcastIdentity() {
    broadcastService.broadcast(SERIALIZER.encode(localMember));
  }

  /**
   * Handles a broadcast message.
   */
  private void handleBroadcastMessage(byte[] message) {
    StatefulMember node = SERIALIZER.decode(message);
    if (members.putIfAbsent(node.id(), node) == null) {
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, node));
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, node));
      sendHeartbeats();
    }
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats() {
    Stream<StatefulMember> clusterNodes = this.members.values()
        .stream()
        .filter(node -> !node.id().equals(getLocalMember().id()));

    Stream<StatefulMember> bootstrapNodes = bootstrapMetadataService.getMetadata()
        .nodes()
        .stream()
        .filter(node -> !node.id().equals(getLocalMember().id()) && !members.containsKey(node.id()))
        .map(node -> new StatefulMember(
            node.id(),
            node.type(),
            node.address(),
            node.zone(),
            node.rack(),
            node.host(),
            node.metadata()));

    byte[] payload = SERIALIZER.encode(new ClusterHeartbeat(
        localMember.id(),
        localMember.type(),
        localMember.zone(),
        localMember.rack(),
        localMember.host(),
        localMember.metadata()));
    return Futures.allOf(Stream.concat(clusterNodes, bootstrapNodes).map(node -> {
      LOGGER.trace("{} - Sending heartbeat: {}", localMember.id(), node.id());
      CompletableFuture<Void> future = sendHeartbeat(node.address(), payload);
      PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(node.id(), n -> new PhiAccrualFailureDetector());
      double phi = failureDetector.phi();
      if (phi >= phiFailureThreshold || (phi == 0.0 && failureDetector.lastUpdated() > 0 && System.currentTimeMillis() - failureDetector.lastUpdated() > failureTimeout)) {
        if (node.getState() == State.ACTIVE) {
          deactivateNode(node);
        }
      } else {
        if (node.getState() == State.INACTIVE) {
          activateNode(node);
        }
      }
      return future.exceptionally(v -> null);
    }).collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(Address address, byte[] payload) {
    return messagingService.sendAndReceive(address, HEARTBEAT_MESSAGE, payload).whenComplete((response, error) -> {
      if (error == null) {
        Collection<StatefulMember> nodes = SERIALIZER.decode(response);
        boolean sendHeartbeats = false;
        for (StatefulMember node : nodes) {
          if (this.members.putIfAbsent(node.id(), node) == null) {
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, node));
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, node));
            sendHeartbeats = true;
          }
        }
        if (sendHeartbeats) {
          sendHeartbeats();
        }
      } else {
        LOGGER.debug("{} - Sending heartbeat to {} failed", localMember.id(), address, error);
      }
    }).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Address address, byte[] message) {
    ClusterHeartbeat heartbeat = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat: {}", localMember.id(), heartbeat.memberId());
    failureDetectors.computeIfAbsent(heartbeat.memberId(), n -> new PhiAccrualFailureDetector()).report();
    activateNode(new StatefulMember(
        heartbeat.memberId(),
        heartbeat.nodeType(),
        address,
        heartbeat.zone(),
        heartbeat.rack(),
        heartbeat.host(),
        heartbeat.metadata()));
    return SERIALIZER.encode(members.values().stream()
        .filter(node -> node.type() == Member.Type.EPHEMERAL)
        .collect(Collectors.toList()));
  }

  /**
   * Activates the given node.
   */
  private void activateNode(Member member) {
    if (member.type() == Member.Type.PERSISTENT && !persistentMetadataService.getMetadata().nodes().contains(member)) {
      return;
    }

    StatefulMember existingNode = members.get(member.id());
    if (existingNode == null) {
      StatefulMember statefulNode = new StatefulMember(
          member.id(),
          member.type(),
          member.address(),
          member.zone(),
          member.rack(),
          member.host(),
          member.metadata());
      LOGGER.info("{} - Node activated: {}", localMember.id(), statefulNode);
      statefulNode.setState(State.ACTIVE);
      members.put(statefulNode.id(), statefulNode);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, statefulNode));
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, statefulNode));
      sendHeartbeat(member.address(), SERIALIZER.encode(new ClusterHeartbeat(
          localMember.id(),
          localMember.type(),
          localMember.zone(),
          localMember.rack(),
          localMember.host(),
          localMember.metadata())));
    } else if (existingNode.getState() == State.INACTIVE) {
      LOGGER.info("{} - Node activated: {}", localMember.id(), existingNode);
      existingNode.setState(State.ACTIVE);
      post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ACTIVATED, existingNode));
    }
  }

  /**
   * Deactivates the given node.
   */
  private void deactivateNode(Member member) {
    StatefulMember existingNode = members.get(member.id());
    if (existingNode != null && existingNode.getState() == State.ACTIVE) {
      LOGGER.info("{} - Node deactivated: {}", localMember.id(), existingNode);
      existingNode.setState(State.INACTIVE);
      switch (existingNode.type()) {
        case PERSISTENT:
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingNode));
          break;
        case EPHEMERAL:
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingNode));
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, existingNode));
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  /**
   * Handles a cluster metadata change event.
   */
  private void handleMetadataEvent(ClusterMetadataEvent event) {
    // Iterate through all bootstrap nodes and add any missing data nodes, triggering NODE_ADDED events.
    // Collect the bootstrap node IDs into a set.
    Set<MemberId> bootstrapNodes = event.subject().nodes().stream()
        .map(node -> {
          StatefulMember existingNode = members.get(node.id());
          if (existingNode == null) {
            StatefulMember newMember = new StatefulMember(
                node.id(),
                node.type(),
                node.address(),
                node.zone(),
                node.rack(),
                node.host(),
                node.metadata());
            members.put(newMember.id(), newMember);
            LOGGER.info("{} - Node added: {}", localMember.id(), newMember);
            post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_ADDED, newMember));
          }
          return node.id();
        }).collect(Collectors.toSet());

    // Filter the set of core node IDs from the local node information.
    Set<MemberId> dataNodes = members.entrySet().stream()
        .filter(entry -> entry.getValue().type() == Member.Type.PERSISTENT)
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet());

    // Compute the set of local data nodes missing in the set of bootstrap nodes.
    Set<MemberId> missingNodes = Sets.difference(dataNodes, bootstrapNodes);

    // For each missing data node, remove the node and trigger a NODE_REMOVED event.
    for (MemberId memberId : missingNodes) {
      StatefulMember existingNode = members.remove(memberId);
      if (existingNode != null) {
        if (existingNode.getState() == State.ACTIVE) {
          LOGGER.info("{} - Node deactivated: {}", localMember.id(), existingNode);
          post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_DEACTIVATED, existingNode));
        }
        LOGGER.info("{} - Node removed: {}", localMember.id(), existingNode);
        post(new ClusterMembershipEvent(ClusterMembershipEvent.Type.MEMBER_REMOVED, existingNode));
      }
    }
  }

  @Override
  public CompletableFuture<ClusterMembershipService> start() {
    if (started.compareAndSet(false, true)) {
      persistentMetadataService.addListener(metadataEventListener);
      broadcastService.addListener(broadcastListener);
      LOGGER.info("{} - Node activated: {}", localMember.id(), localMember);
      localMember.setState(State.ACTIVE);
      members.put(localMember.id(), localMember);
      persistentMetadataService.getMetadata().nodes()
          .forEach(node -> members.putIfAbsent(node.id(), new StatefulMember(
              node.id(),
              node.type(),
              node.address(),
              node.zone(),
              node.rack(),
              node.host(),
              node.metadata())));
      messagingService.registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatExecutor);

      ComposableFuture<Void> future = new ComposableFuture<>();
      broadcastIdentity();
      sendHeartbeats().whenComplete((r, e) -> {
        future.complete(null);
      });

      heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(() -> {
        broadcastIdentity();
        sendHeartbeats();
      }, 0, heartbeatInterval, TimeUnit.MILLISECONDS);

      return future.thenApply(v -> {
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
      heartbeatScheduler.shutdownNow();
      heartbeatExecutor.shutdownNow();
      LOGGER.info("{} - Node deactivated: {}", localMember.id(), localMember);
      localMember.setState(State.INACTIVE);
      members.clear();
      heartbeatFuture.cancel(true);
      messagingService.unregisterHandler(HEARTBEAT_MESSAGE);
      persistentMetadataService.removeListener(metadataEventListener);
      LOGGER.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }
}
