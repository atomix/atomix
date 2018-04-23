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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupFactory;
import io.atomix.primitive.partition.PartitionGroups;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.impl.DefaultSessionIdService;
import io.atomix.primitive.session.impl.ReplicatedSessionIdService;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default partition service.
 */
public class DefaultPartitionService implements ManagedPartitionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);
  private static final String BOOTSTRAP_SUBJECT = "partition-bootstrap";

  private final ClusterService clusterService;
  private final ClusterMessagingService messagingService;
  private final PrimitiveTypeRegistry primitiveTypeRegistry;
  private final Serializer serializer;
  private ManagedPartitionGroup<?> systemGroup;
  private ManagedPartitionGroup<?> defaultGroup;
  private final Map<String, ManagedPartitionGroup<?>> groups = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultPartitionService(
      ClusterService clusterService,
      ClusterMessagingService messagingService,
      PrimitiveTypeRegistry primitiveTypeRegistry,
      ManagedPartitionGroup systemGroup,
      Collection<ManagedPartitionGroup> groups) {
    this.clusterService = clusterService;
    this.messagingService = messagingService;
    this.primitiveTypeRegistry = primitiveTypeRegistry;
    this.systemGroup = systemGroup;
    this.defaultGroup = groups.iterator().next();
    groups.forEach(g -> this.groups.put(g.name(), g));

    KryoNamespace.Builder builder = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(NodeId.class)
        .register(PartitionGroupInfo.class)
        .register(PartitionGroupConfig.class);
    for (PartitionGroupFactory factory : PartitionGroups.getGroupFactories()) {
      builder.register(factory.configClass());
    }
    serializer = Serializer.using(builder.build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends PrimitiveProtocol> PartitionGroup<P> getSystemPartitionGroup() {
    return (PartitionGroup<P>) systemGroup;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends PrimitiveProtocol> PartitionGroup<P> getDefaultPartitionGroup() {
    return (PartitionGroup<P>) defaultGroup;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends PrimitiveProtocol> PartitionGroup<P> getPartitionGroup(String name) {
    return (PartitionGroup<P>) groups.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup<?>> getPartitionGroups() {
    return (Collection) groups.values();
  }

  /**
   * Bootstraps the service.
   */
  private CompletableFuture<Void> bootstrap() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Futures.allOf(clusterService.getNodes().stream()
        .map(node -> bootstrap(node))
        .collect(Collectors.toList()))
        .whenComplete((result, error) -> {
          if (error == null) {
            if (systemGroup == null) {
              future.completeExceptionally(new ConfigurationException("Failed to locate system partition group"));
            } else if (groups.isEmpty()) {
              future.completeExceptionally(new ConfigurationException("Failed to locate partition groups"));
            } else {
              future.complete(null);
            }
          } else {
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  /**
   * Bootstraps the service from the given node.
   */
  private CompletableFuture<Void> bootstrap(Node node) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    messagingService.<NodeId, PartitionGroupInfo>send(BOOTSTRAP_SUBJECT, clusterService.getLocalNode().id(), serializer::encode, serializer::decode, node.id())
        .whenComplete((info, error) -> {
          if (error == null) {
            if (systemGroup != null && info.systemGroup != null &&
                (!systemGroup.name().equals(info.systemGroup.getName()) || !systemGroup.type().equals(info.systemGroup.getType()))) {
              future.completeExceptionally(new ConfigurationException("Duplicate system group detected"));
            } else if (systemGroup == null && info.systemGroup != null) {
              systemGroup = PartitionGroups.createGroup(info.systemGroup);
            }

            for (PartitionGroupConfig groupConfig : info.groups) {
              PartitionGroup group = groups.get(groupConfig.getName());
              if (group == null) {
                groups.put(groupConfig.getName(), PartitionGroups.createGroup(groupConfig));
              } else if (!group.type().equals(groupConfig.getType())) {
                future.completeExceptionally(new ConfigurationException("Duplicate partition group " + groupConfig.getName() + " detected"));
              }
            }
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<PartitionService> start() {
    return bootstrap()
        .thenCompose(v -> systemGroup.open(new DefaultPartitionManagementService(
            clusterService,
            messagingService,
            primitiveTypeRegistry,
            new HashBasedPrimaryElectionService(clusterService, messagingService),
            new DefaultSessionIdService())))
        .thenCompose(v -> {
          ManagedPrimaryElectionService systemElectionService = new DefaultPrimaryElectionService(systemGroup);
          ManagedSessionIdService systemSessionIdService = new ReplicatedSessionIdService(systemGroup);
          return systemElectionService.start()
              .thenCompose(v2 -> systemSessionIdService.start())
              .thenApply(v2 -> new DefaultPartitionManagementService(
                  clusterService,
                  messagingService,
                  primitiveTypeRegistry,
                  systemElectionService,
                  systemSessionIdService));
        })
        .thenCompose(managementService -> {
          List<CompletableFuture> futures = groups.values().stream()
              .map(g -> g.open(managementService))
              .collect(Collectors.toList());
          return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
            LOGGER.info("Started");
            return this;
          });
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    List<CompletableFuture<Void>> futures = groups.values().stream()
        .map(ManagedPartitionGroup::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      LOGGER.info("Stopped");
    });
  }

  private static class PartitionGroupInfo {
    private final PartitionGroupConfig systemGroup;
    private final Collection<PartitionGroupConfig> groups;

    PartitionGroupInfo(PartitionGroupConfig systemGroup, Collection<PartitionGroupConfig> groups) {
      this.systemGroup = systemGroup;
      this.groups = groups;
    }
  }
}
