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
package io.atomix.cluster.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicLongMap;
import io.atomix.cluster.AtomixClusterBuilder;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.impl.AddressSerializer;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Cluster membership provider that uses multicast for member discovery.
 * <p>
 * This implementation uses the {@link io.atomix.cluster.messaging.BroadcastService} internally and thus requires that
 * multicast is {@link AtomixClusterBuilder#withMulticastEnabled() enabled} on the Atomix instance. Membership is
 * determined by each node broadcasting to a multicast group, and phi accrual failure detectors are used to detect nodes
 * joining and leaving the cluster.
 */
public class MulticastDiscoveryProvider
    extends AbstractListenerManager<NodeDiscoveryEvent, NodeDiscoveryEventListener>
    implements NodeDiscoveryProvider {

  public static final Type TYPE = new Type();

  /**
   * Returns a new multicast member location provider builder.
   *
   * @return a new multicast location provider builder
   */
  public static MulticastDiscoveryBuilder builder() {
    return new MulticastDiscoveryBuilder();
  }

  /**
   * Broadcast member location provider type.
   */
  public static class Type implements NodeDiscoveryProvider.Type<MulticastDiscoveryConfig> {
    private static final String NAME = "multicast";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public MulticastDiscoveryConfig newConfig() {
      return new MulticastDiscoveryConfig();
    }

    @Override
    public NodeDiscoveryProvider newProvider(MulticastDiscoveryConfig config) {
      return new MulticastDiscoveryProvider(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MulticastDiscoveryProvider.class);
  private static final Serializer SERIALIZER = Serializer.builder()
      .addType(Node.class)
      .addType(NodeId.class)
      .addSerializer(new AddressSerializer(), Address.class)
      .build();

  private static final String DISCOVERY_SUBJECT = "atomix-discovery";

  private final MulticastDiscoveryConfig config;
  private volatile BootstrapService bootstrap;

  private final ScheduledExecutorService broadcastScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-broadcast", LOGGER));
  private volatile ScheduledFuture<?> broadcastFuture;
  private final Consumer<byte[]> broadcastListener = message -> broadcastScheduler.execute(() -> handleBroadcastMessage(message));

  private final Map<NodeId, Node> nodes = Maps.newConcurrentMap();
  private final AtomicLongMap<NodeId> updateTimes = AtomicLongMap.create();

  public MulticastDiscoveryProvider() {
    this(new MulticastDiscoveryConfig());
  }

  public MulticastDiscoveryProvider(MulticastDiscoveryConfig config) {
    this.config = checkNotNull(config);
  }

  @Override
  public MulticastDiscoveryConfig config() {
    return config;
  }

  @Override
  public Set<Node> getNodes() {
    return ImmutableSet.copyOf(nodes.values());
  }

  private void handleBroadcastMessage(byte[] message) {
    Node node = SERIALIZER.decode(message);
    Node oldNode = nodes.put(node.id(), node);
    if (oldNode != null && !oldNode.id().equals(node.id())) {
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, oldNode));
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
    } else if (oldNode == null) {
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
    }
    updateTimes.put(node.id(), System.currentTimeMillis());
  }

  private void broadcastNode(Node localNode) {
    bootstrap.getBroadcastService().broadcast(DISCOVERY_SUBJECT, SERIALIZER.encode(localNode));
    expireNodes();
  }

  private void expireNodes() {
    Iterator<Map.Entry<NodeId, Node>> iterator = nodes.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<NodeId, Node> entry = iterator.next();
      if (System.currentTimeMillis() - updateTimes.get(entry.getKey()) > config.getFailureTimeout().toMillis()) {
        iterator.remove();
        post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, entry.getValue()));
      }
    }
  }

  @Override
  public CompletableFuture<Void> join(BootstrapService bootstrap, Node localNode) {
    if (nodes.putIfAbsent(localNode.id(), localNode) == null) {
      this.bootstrap = bootstrap;
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, localNode));
      bootstrap.getBroadcastService().addListener(DISCOVERY_SUBJECT, broadcastListener);
      broadcastFuture = broadcastScheduler.scheduleAtFixedRate(
          () -> broadcastNode(localNode),
          config.getBroadcastInterval().toMillis(),
          config.getBroadcastInterval().toMillis(),
          TimeUnit.MILLISECONDS);
      broadcastNode(localNode);
      LOGGER.info("Joined");
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave(Node localNode) {
    if (nodes.remove(localNode.id()) != null) {
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, localNode));
      bootstrap.getBroadcastService().removeListener(DISCOVERY_SUBJECT, broadcastListener);
      ScheduledFuture<?> broadcastFuture = this.broadcastFuture;
      if (broadcastFuture != null) {
        broadcastFuture.cancel(false);
      }
      LOGGER.info("Left");
    }
    return CompletableFuture.completedFuture(null);
  }
}
