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
import io.atomix.cluster.Cluster;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ManagedCluster;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.MessagingService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default cluster implementation.
 */
public class DefaultCluster implements ManagedCluster {
  private final ClusterMetadata clusterMetadata;
  private final MessagingService messagingService;
  private final AtomicBoolean open = new AtomicBoolean();
  private final Map<NodeId, DefaultNode> nodes = Maps.newConcurrentMap();
  private final Set<ClusterEventListener> eventListeners = Sets.newCopyOnWriteArraySet();

  public DefaultCluster(ClusterMetadata clusterMetadata, MessagingService messagingService) {
    this.clusterMetadata = checkNotNull(clusterMetadata, "clusterMetadata cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    clusterMetadata.bootstrapNodes().forEach(n -> nodes.put(n.id(), (DefaultNode) n));
  }

  @Override
  public Node localNode() {
    return clusterMetadata.localNode();
  }

  @Override
  public Set<Node> nodes() {
    return ImmutableSet.copyOf(nodes.values());
  }

  @Override
  public Node node(NodeId nodeId) {
    return nodes.get(nodeId);
  }

  @Override
  public ClusterMetadata metadata() {
    return clusterMetadata;
  }

  @Override
  public void addListener(ClusterEventListener listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(ClusterEventListener listener) {
    eventListeners.remove(listener);
  }

  @Override
  public CompletableFuture<Cluster> open() {
    open.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
