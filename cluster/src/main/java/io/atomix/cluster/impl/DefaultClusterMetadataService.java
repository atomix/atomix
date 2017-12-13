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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ManagedClusterMetadataService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default cluster metadata service.
 */
public class DefaultClusterMetadataService
    extends AbstractListenerManager<ClusterMetadataEvent, ClusterMetadataEventListener>
    implements ManagedClusterMetadataService {

  private static final String BOOTSTRAP_MESSAGE = "atomix-cluster-metadata-bootstrap";
  private static final String UPDATE_MESSAGE = "atomix-cluster-metadata-update";
  private static final String ADVERTISEMENT_MESSAGE = "atomix-cluster-metadata-advertisement";
  private static final int HEARTBEAT_INTERVAL = 1000;

  private static final Serializer SERIALIZER = Serializer.using(
      KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(Node.class)
          .register(NodeId.class)
          .register(Node.Type.class)
          .register(LogicalTimestamp.class)
          .register(NodeUpdate.class)
          .register(ClusterMetadataAdvertisement.class)
          .register(NodeDigest.class)
          .build("ClusterMetadataService"));

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Map<NodeId, ReplicatedNode> nodes = Maps.newConcurrentMap();
  private final MessagingService messagingService;
  private final LogicalClock clock = new LogicalClock();
  private final AtomicBoolean open = new AtomicBoolean();

  private final ScheduledExecutorService messageScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-metadata-sender", log));
  private final ExecutorService messageExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-metadata-receiver", log));
  private ScheduledFuture<?> metadataFuture;

  public DefaultClusterMetadataService(ClusterMetadata metadata, MessagingService messagingService) {
    metadata.bootstrapNodes().forEach(node -> nodes.put(node.id(), (ReplicatedNode) node));
    this.messagingService = messagingService;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ClusterMetadata getMetadata() {
    return new ClusterMetadata(ImmutableList.copyOf(nodes.values().stream()
        .filter(node -> node.tombstone())
        .collect(Collectors.toList())));
  }

  @Override
  public void addNode(Node node) {
    ReplicatedNode replicatedNode = new ReplicatedNode(node.id(), node.type(), node.endpoint(), clock.increment(), false);
    if (nodes.putIfAbsent(node.id(), replicatedNode) == null) {
      broadcastUpdate(replicatedNode);
      post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
    }
  }

  @Override
  public void removeNode(Node node) {
    ReplicatedNode replicatedNode = nodes.get(node.id());
    if (replicatedNode != null) {
      replicatedNode = new ReplicatedNode(node.id(), node.type(), node.endpoint(), clock.increment(), true);
      nodes.put(node.id(), replicatedNode);
      broadcastUpdate(replicatedNode);
      post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
    }
  }

  /**
   * Bootstraps the cluster metadata.
   */
  private CompletableFuture<Void> bootstrap() {
    Set<Endpoint> peers = nodes.values().stream()
        .map(Node::endpoint)
        .filter(endpoint -> !endpoint.equals(messagingService.endpoint()))
        .collect(Collectors.toSet());
    final int totalPeers = peers.size();
    AtomicBoolean successful = new AtomicBoolean();
    AtomicInteger totalCount = new AtomicInteger();
    AtomicReference<Throwable> lastError = new AtomicReference<>();

    // Iterate through all of the peers and send a bootstrap request. On the first peer that returns
    // a successful bootstrap response, complete the future. Otherwise, if no peers respond with any
    // successful bootstrap response, the future will be completed with the last exception.
    CompletableFuture<Void> future = new CompletableFuture<>();
    peers.forEach(peer -> {
      bootstrap(peer).whenComplete((result, error) -> {
        if (error == null) {
          if (successful.compareAndSet(false, true)) {
            future.complete(null);
          } else if (totalCount.incrementAndGet() == totalPeers) {
            Throwable e = lastError.get();
            if (e != null) {
              future.completeExceptionally(e);
            }
          }
        } else {
          if (!successful.get() && totalCount.incrementAndGet() == totalPeers) {
            future.completeExceptionally(error);
          } else {
            lastError.set(error);
          }
        }
      });
    });
    return future;
  }

  /**
   * Requests a bootstrap from the given endpoint.
   */
  private CompletableFuture<Void> bootstrap(Endpoint endpoint) {
    return messagingService.sendAndReceive(endpoint, BOOTSTRAP_MESSAGE, new byte[0])
        .thenAccept(response -> nodes.putAll(SERIALIZER.decode(response)));
  }

  /**
   * Handles a bootstrap request.
   */
  private byte[] handleBootstrap(Endpoint endpoint, byte[] payload) {
    return SERIALIZER.encode(nodes);
  }

  /**
   * Broadcasts the given update to all peers.
   */
  private void broadcastUpdate(ReplicatedNode update) {
    nodes.values().stream()
        .map(Node::endpoint)
        .forEach(endpoint -> sendUpdate(endpoint, update));
  }

  /**
   * Sends the given update to the given node.
   */
  private void sendUpdate(Endpoint endpoint, ReplicatedNode update) {
    messagingService.sendAsync(endpoint, UPDATE_MESSAGE, SERIALIZER.encode(update));
  }

  /**
   * Handles an update from another node.
   */
  private void handleUpdate(Endpoint endpoint, byte[] payload) {
    ReplicatedNode update = SERIALIZER.decode(payload);
    ReplicatedNode node = nodes.get(update.id());
    if (node == null || node.timestamp().isOlderThan(update.timestamp())) {
      nodes.put(update.id(), update);
      post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
    }
  }

  /**
   * Sends anti-entropy advertisements to a random node.
   */
  private void sendAdvertisement() {
    pickRandomPeer().ifPresent(this::sendAdvertisement);
  }

  /**
   * Sends an anti-entropy advertisement to the given node.
   */
  private void sendAdvertisement(Endpoint endpoint) {
    ClusterMetadataAdvertisement advertisement = new ClusterMetadataAdvertisement(
        Maps.transformValues(nodes, node -> new NodeDigest(node.timestamp(), node.tombstone())));
    messagingService.sendAndReceive(endpoint, ADVERTISEMENT_MESSAGE, SERIALIZER.encode(advertisement))
        .whenComplete((response, error) -> {
          if (error == null) {
            List<NodeId> nodes = SERIALIZER.decode(response);
            for (NodeId nodeId : nodes) {
              ReplicatedNode node = this.nodes.get(nodeId);
              if (node != null) {
                sendUpdate(endpoint, node);
              }
            }
          } else {
            log.warn("Anti-entropy advertisement to {} failed!", endpoint);
          }
        });
  }

  /**
   * Selects a random peer to which to send an anti-entropy advertisement.
   */
  private Optional<Endpoint> pickRandomPeer() {
    List<Endpoint> nodes = this.nodes.values()
        .stream()
        .map(Node::endpoint)
        .filter(endpoint -> !endpoint.equals(messagingService.endpoint()))
        .collect(Collectors.toList());
    Collections.shuffle(nodes);
    return nodes.stream().findFirst();
  }

  /**
   * Handles an anti-entropy advertisement.
   */
  private byte[] handleAdvertisement(Endpoint endpoint, byte[] payload) {
    ClusterMetadataAdvertisement advertisement = SERIALIZER.decode(payload);
    Set<NodeId> staleNodes = nodes.values().stream().map(node -> {
      NodeDigest digest = advertisement.digest(node.id());
      if (digest == null || node.isNewerThan(digest.timestamp())) {
        sendUpdate(endpoint, node);
      } else if (digest.isNewerThan(node.timestamp())) {
        if (digest.tombstone()) {
          if (!node.tombstone()) {
            nodes.put(node.id(), new ReplicatedNode(node.id(), node.type(), node.endpoint(), digest.timestamp(), true));
            post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
          }
        } else {
          return node.id();
        }
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toSet());
    return SERIALIZER.encode(Sets.union(Sets.difference(advertisement.digests(), nodes.keySet()), staleNodes));
  }

  @Override
  public CompletableFuture<ClusterMetadataService> open() {
    if (open.compareAndSet(false, true)) {
      return bootstrap().whenComplete((result, error) -> {
        registerMessageHandlers();
        metadataFuture = messageScheduler.scheduleWithFixedDelay(this::sendAdvertisement, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        log.info("Started");
      }).thenApply(v -> this);
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  /**
   * Registers cluster message handlers.
   */
  private void registerMessageHandlers() {
    messagingService.registerHandler(BOOTSTRAP_MESSAGE, this::handleBootstrap, messageExecutor);
    messagingService.registerHandler(UPDATE_MESSAGE, this::handleUpdate, messageExecutor);
    messagingService.registerHandler(ADVERTISEMENT_MESSAGE, this::handleAdvertisement, messageExecutor);
  }

  /**
   * Unregisters cluster message handlers.
   */
  private void unregisterMessageHandlers() {
    messagingService.unregisterHandler(BOOTSTRAP_MESSAGE);
    messagingService.unregisterHandler(UPDATE_MESSAGE);
    messagingService.unregisterHandler(ADVERTISEMENT_MESSAGE);
  }

  @Override
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      messageScheduler.shutdownNow();
      messageExecutor.shutdownNow();
      metadataFuture.cancel(true);
      unregisterMessageHandlers();
    }
    log.info("Stopped");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
