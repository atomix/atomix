// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.impl;

import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.discovery.ManagedNodeDiscoveryService;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.NodeDiscoveryEvent;
import io.atomix.cluster.discovery.NodeDiscoveryEventListener;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.utils.event.AbstractListenerManager;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default node discovery service.
 */
public class DefaultNodeDiscoveryService
    extends AbstractListenerManager<NodeDiscoveryEvent, NodeDiscoveryEventListener>
    implements ManagedNodeDiscoveryService {

  private final BootstrapService bootstrapService;
  private final Node localNode;
  private final NodeDiscoveryProvider provider;
  private final AtomicBoolean started = new AtomicBoolean();
  private final NodeDiscoveryEventListener discoveryEventListener = this::post;

  public DefaultNodeDiscoveryService(BootstrapService bootstrapService, Node localNode, NodeDiscoveryProvider provider) {
    this.bootstrapService = bootstrapService;
    this.localNode = localNode;
    this.provider = provider;
  }

  @Override
  public Set<Node> getNodes() {
    return provider.getNodes();
  }

  @Override
  public CompletableFuture<NodeDiscoveryService> start() {
    if (started.compareAndSet(false, true)) {
      provider.addListener(discoveryEventListener);
      Node node = Node.builder().withId(localNode.id().id()).withAddress(localNode.address()).build();
      return provider.join(bootstrapService, node).thenApply(v -> this);
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
      return provider.leave(localNode).thenRun(() -> {
        provider.removeListener(discoveryEventListener);
      });
    }
    return CompletableFuture.completedFuture(null);
  }
}
