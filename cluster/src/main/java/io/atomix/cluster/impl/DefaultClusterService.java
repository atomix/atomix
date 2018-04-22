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
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.PersistentMetadataService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.Node.State;
import io.atomix.cluster.NodeId;
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
public class DefaultClusterService
    extends AbstractListenerManager<ClusterEvent, ClusterEventListener>
    implements ManagedClusterService {

  private static final Logger LOGGER = getLogger(DefaultClusterService.class);

  private static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
  private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;
  private static final long DEFAULT_FAILURE_TIME = 10000;
  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-heartbeat";

  private int heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;

  private int phiFailureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;

  private static final Serializer SERIALIZER = Serializer.using(
      KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(NodeId.class)
          .register(Node.Type.class)
          .register(Node.State.class)
          .register(ClusterHeartbeat.class)
          .register(StatefulNode.class)
          .register(new DefaultPersistentMetadataService.AddressSerializer(), Address.class)
          .build("ClusterService"));

  private final MessagingService messagingService;
  private final BroadcastService broadcastService;
  private final BootstrapMetadataService bootstrapMetadataService;
  private final PersistentMetadataService persistentMetadataService;
  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulNode localNode;
  private final Map<NodeId, StatefulNode> nodes = Maps.newConcurrentMap();
  private final Map<NodeId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final ClusterMetadataEventListener metadataEventListener = this::handleMetadataEvent;
  private final Consumer<byte[]> broadcastListener = this::handleBroadcastMessage;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public DefaultClusterService(
      Node localNode,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService) {
    this.bootstrapMetadataService = checkNotNull(bootstrapMetadataService, "bootstrapMetadataService cannot be null");
    this.persistentMetadataService = checkNotNull(persistentMetadataService, "coreMetadataService cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.broadcastService = checkNotNull(broadcastService, "broadcastService cannot be null");
    this.localNode = new StatefulNode(
        localNode.id(),
        localNode.type(),
        localNode.address(),
        localNode.zone(),
        localNode.rack(),
        localNode.host(),
        localNode.tags());
  }

  @Override
  public Node getLocalNode() {
    return localNode;
  }

  @Override
  public Set<Node> getNodes() {
    return ImmutableSet.copyOf(nodes.values()
        .stream()
        .filter(node -> node.type() == Node.Type.PERSISTENT || node.getState() == State.ACTIVE)
        .collect(Collectors.toList()));
  }

  @Override
  public Node getNode(NodeId nodeId) {
    Node node = nodes.get(nodeId);
    return node != null && (node.type() == Node.Type.PERSISTENT || node.getState() == State.ACTIVE) ? node : null;
  }

  /**
   * Broadcasts this node's identity.
   */
  private void broadcastIdentity() {
    broadcastService.broadcast(SERIALIZER.encode(localNode));
  }

  /**
   * Handles a broadcast message.
   */
  private void handleBroadcastMessage(byte[] message) {
    StatefulNode node = SERIALIZER.decode(message);
    if (nodes.putIfAbsent(node.id(), node) == null) {
      post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, node));
      post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, node));
      sendHeartbeats();
    }
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats() {
    Stream<StatefulNode> clusterNodes = this.nodes.values()
        .stream()
        .filter(node -> !node.id().equals(getLocalNode().id()));

    Stream<StatefulNode> bootstrapNodes = bootstrapMetadataService.getMetadata()
        .nodes()
        .stream()
        .filter(node -> !node.id().equals(getLocalNode().id()) && !nodes.containsKey(node.id()))
        .map(node -> new StatefulNode(
            node.id(),
            node.type(),
            node.address(),
            node.zone(),
            node.rack(),
            node.host(),
            node.tags()));

    byte[] payload = SERIALIZER.encode(new ClusterHeartbeat(
        localNode.id(),
        localNode.type(),
        localNode.zone(),
        localNode.rack(),
        localNode.host(),
        localNode.tags()));
    return Futures.allOf(Stream.concat(clusterNodes, bootstrapNodes).map(node -> {
      LOGGER.trace("{} - Sending heartbeat: {}", localNode.id(), node.id());
      CompletableFuture<Void> future = sendHeartbeat(node.address(), payload);
      PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(node.id(), n -> new PhiAccrualFailureDetector());
      double phi = failureDetector.phi();
      if (phi >= phiFailureThreshold || (phi == 0.0 && failureDetector.lastUpdated() > 0 && System.currentTimeMillis() - failureDetector.lastUpdated() > DEFAULT_FAILURE_TIME)) {
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
        Collection<StatefulNode> nodes = SERIALIZER.decode(response);
        boolean sendHeartbeats = false;
        for (StatefulNode node : nodes) {
          if (this.nodes.putIfAbsent(node.id(), node) == null) {
            post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, node));
            post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, node));
            sendHeartbeats = true;
          }
        }
        if (sendHeartbeats) {
          sendHeartbeats();
        }
      } else {
        LOGGER.debug("{} - Sending heartbeat to {} failed", localNode.id(), address, error);
      }
    }).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Address address, byte[] message) {
    ClusterHeartbeat heartbeat = SERIALIZER.decode(message);
    LOGGER.trace("{} - Received heartbeat: {}", localNode.id(), heartbeat.nodeId());
    failureDetectors.computeIfAbsent(heartbeat.nodeId(), n -> new PhiAccrualFailureDetector()).report();
    activateNode(new StatefulNode(
        heartbeat.nodeId(),
        heartbeat.nodeType(),
        address,
        heartbeat.zone(),
        heartbeat.rack(),
        heartbeat.host(),
        heartbeat.tags()));
    return SERIALIZER.encode(nodes.values().stream()
        .filter(node -> node.type() == Node.Type.CLIENT)
        .collect(Collectors.toList()));
  }

  /**
   * Activates the given node.
   */
  private void activateNode(Node node) {
    StatefulNode existingNode = nodes.get(node.id());
    if (existingNode == null) {
      StatefulNode statefulNode = new StatefulNode(
          node.id(),
          node.type(),
          node.address(),
          node.zone(),
          node.rack(),
          node.host(),
          node.tags());
      LOGGER.info("{} - Node activated: {}", localNode.id(), statefulNode);
      statefulNode.setState(State.ACTIVE);
      nodes.put(statefulNode.id(), statefulNode);
      post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, statefulNode));
      post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, statefulNode));
      sendHeartbeat(node.address(), SERIALIZER.encode(new ClusterHeartbeat(
          localNode.id(),
          localNode.type(),
          localNode.zone(),
          localNode.rack(),
          localNode.host(),
          localNode.tags())));
    } else if (existingNode.getState() == State.INACTIVE) {
      LOGGER.info("{} - Node activated: {}", localNode.id(), existingNode);
      existingNode.setState(State.ACTIVE);
      post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, existingNode));
    }
  }

  /**
   * Deactivates the given node.
   */
  private void deactivateNode(Node node) {
    StatefulNode existingNode = nodes.get(node.id());
    if (existingNode != null && existingNode.getState() == State.ACTIVE) {
      LOGGER.info("{} - Node deactivated: {}", localNode.id(), existingNode);
      existingNode.setState(State.INACTIVE);
      switch (existingNode.type()) {
        case PERSISTENT:
          post(new ClusterEvent(ClusterEvent.Type.NODE_DEACTIVATED, existingNode));
          break;
        case EPHEMERAL:
        case CLIENT:
          post(new ClusterEvent(ClusterEvent.Type.NODE_DEACTIVATED, existingNode));
          post(new ClusterEvent(ClusterEvent.Type.NODE_REMOVED, existingNode));
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
    Set<NodeId> bootstrapNodes = event.subject().nodes().stream()
        .map(node -> {
          StatefulNode existingNode = nodes.get(node.id());
          if (existingNode == null) {
            StatefulNode newNode = new StatefulNode(
                node.id(),
                node.type(),
                node.address(),
                node.zone(),
                node.rack(),
                node.host(),
                node.tags());
            nodes.put(newNode.id(), newNode);
            post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, newNode));
          }
          return node.id();
        }).collect(Collectors.toSet());

    // Filter the set of core node IDs from the local node information.
    Set<NodeId> dataNodes = nodes.entrySet().stream()
        .filter(entry -> entry.getValue().type() == Node.Type.PERSISTENT)
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet());

    // Compute the set of local data nodes missing in the set of bootstrap nodes.
    Set<NodeId> missingNodes = Sets.difference(dataNodes, bootstrapNodes);

    // For each missing data node, remove the node and trigger a NODE_REMOVED event.
    for (NodeId nodeId : missingNodes) {
      StatefulNode existingNode = nodes.remove(nodeId);
      if (existingNode != null) {
        post(new ClusterEvent(ClusterEvent.Type.NODE_REMOVED, existingNode));
      }
    }
  }

  @Override
  public CompletableFuture<ClusterService> start() {
    if (started.compareAndSet(false, true)) {
      persistentMetadataService.addListener(metadataEventListener);
      broadcastService.addListener(broadcastListener);
      LOGGER.info("{} - Node activated: {}", localNode.id(), localNode);
      localNode.setState(State.ACTIVE);
      nodes.put(localNode.id(), localNode);
      persistentMetadataService.getMetadata().nodes()
          .forEach(node -> nodes.putIfAbsent(node.id(), new StatefulNode(
              node.id(),
              node.type(),
              node.address(),
              node.zone(),
              node.rack(),
              node.host(),
              node.tags())));
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
      LOGGER.info("{} - Node deactivated: {}", localNode.id(), localNode);
      localNode.setState(State.INACTIVE);
      nodes.clear();
      heartbeatFuture.cancel(true);
      messagingService.unregisterHandler(HEARTBEAT_MESSAGE);
      persistentMetadataService.removeListener(metadataEventListener);
      LOGGER.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }
}
