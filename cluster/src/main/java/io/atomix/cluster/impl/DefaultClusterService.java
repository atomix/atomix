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
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.Node.State;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.event.AbstractListenerManager;
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
import java.util.stream.Collectors;

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
  private static final long DEFAULT_FAILURE_TIME = 1000;
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
          .register(new DefaultClusterMetadataService.EndpointSerializer(), Endpoint.class)
          .build("ClusterService"));

  private final MessagingService messagingService;
  private final ClusterMetadataService metadataService;
  private final AtomicBoolean started = new AtomicBoolean();
  private final StatefulNode localNode;
  private final Map<NodeId, StatefulNode> nodes = Maps.newConcurrentMap();
  private final Map<NodeId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private final ClusterMetadataEventListener metadataEventListener = this::handleMetadataEvent;

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  public DefaultClusterService(Node localNode, ClusterMetadataService metadataService, MessagingService messagingService) {
    this.metadataService = checkNotNull(metadataService, "metadataService cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.localNode = new StatefulNode(localNode.id(), localNode.type(), localNode.endpoint());
  }

  @Override
  public Node getLocalNode() {
    return localNode;
  }

  @Override
  public Set<Node> getNodes() {
    return ImmutableSet.copyOf(nodes.values()
        .stream()
        .filter(node -> node.type() == Node.Type.DATA || node.getState() == State.ACTIVE)
        .collect(Collectors.toList()));
  }

  @Override
  public Node getNode(NodeId nodeId) {
    Node node = nodes.get(nodeId);
    return node != null ? node.type() == Node.Type.DATA || node.getState() == State.ACTIVE ? node : null : null;
  }

  /**
   * Sends heartbeats to all peers.
   */
  private void sendHeartbeats() {
    try {
      Set<StatefulNode> peers = nodes.values()
          .stream()
          .filter(node -> !node.id().equals(getLocalNode().id()))
          .collect(Collectors.toSet());
      byte[] payload = SERIALIZER.encode(new ClusterHeartbeat(localNode.id(), localNode.type()));
      peers.forEach((node) -> {
        sendHeartbeat(node.endpoint(), payload);
        PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(node.id(), n -> new PhiAccrualFailureDetector());
        double phi = failureDetector.phi();
        if (phi >= phiFailureThreshold || System.currentTimeMillis() - failureDetector.lastUpdated() > DEFAULT_FAILURE_TIME) {
          if (node.getState() == State.ACTIVE) {
            deactivateNode(node);
          }
        } else {
          if (node.getState() == State.INACTIVE) {
            activateNode(node);
          }
        }
      });
    } catch (Exception e) {
      LOGGER.debug("Failed to send heartbeat", e);
    }
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private void sendHeartbeat(Endpoint endpoint, byte[] payload) {
    messagingService.sendAndReceive(endpoint, HEARTBEAT_MESSAGE, payload).whenComplete((response, error) -> {
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
        LOGGER.trace("Sending heartbeat to {} failed", endpoint, error);
      }
    });
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Endpoint endpoint, byte[] message) {
    ClusterHeartbeat heartbeat = SERIALIZER.decode(message);
    failureDetectors.computeIfAbsent(heartbeat.nodeId(), n -> new PhiAccrualFailureDetector()).report();
    activateNode(new StatefulNode(heartbeat.nodeId(), heartbeat.nodeType(), endpoint));
    return SERIALIZER.encode(nodes.values().stream()
        .filter(node -> node.type() == Node.Type.CLIENT)
        .collect(Collectors.toList()));
  }

  /**
   * Activates the given node.
   */
  private void activateNode(StatefulNode node) {
    StatefulNode existingNode = nodes.get(node.id());
    if (existingNode == null) {
      node.setState(State.ACTIVE);
      nodes.put(node.id(), node);
      post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, node));
      post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, node));
      sendHeartbeat(node.endpoint(), SERIALIZER.encode(new ClusterHeartbeat(localNode.id(), localNode.type())));
    } else if (existingNode.getState() == State.INACTIVE) {
      existingNode.setState(State.ACTIVE);
      post(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, existingNode));
    }
  }

  /**
   * Deactivates the given node.
   */
  private void deactivateNode(StatefulNode node) {
    StatefulNode existingNode = nodes.get(node.id());
    if (existingNode != null && existingNode.getState() == State.ACTIVE) {
      existingNode.setState(State.INACTIVE);
      switch (existingNode.type()) {
        case DATA:
          post(new ClusterEvent(ClusterEvent.Type.NODE_DEACTIVATED, existingNode));
          break;
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
    Set<NodeId> bootstrapNodes = event.subject().bootstrapNodes().stream()
        .map(node -> {
          StatefulNode existingNode = nodes.get(node.id());
          if (existingNode == null) {
            StatefulNode newNode = new StatefulNode(node.id(), node.type(), node.endpoint());
            nodes.put(newNode.id(), newNode);
            post(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, newNode));
          }
          return node.id();
        }).collect(Collectors.toSet());

    // Filter the set of data node IDs from the local node information.
    Set<NodeId> dataNodes = nodes.entrySet().stream()
        .filter(entry -> entry.getValue().type() == Node.Type.DATA)
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
      metadataService.addListener(metadataEventListener);
      localNode.setState(State.ACTIVE);
      nodes.put(localNode.id(), localNode);
      metadataService.getMetadata().bootstrapNodes()
          .forEach(node -> nodes.putIfAbsent(node.id(), new StatefulNode(node.id(), node.type(), node.endpoint())));
      messagingService.registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatExecutor);
      heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(this::sendHeartbeats, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
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
      heartbeatScheduler.shutdownNow();
      heartbeatExecutor.shutdownNow();
      localNode.setState(State.INACTIVE);
      nodes.clear();
      heartbeatFuture.cancel(true);
      messagingService.unregisterHandler(HEARTBEAT_MESSAGE);
      metadataService.removeListener(metadataEventListener);
      LOGGER.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }
}
