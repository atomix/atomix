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
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupFactory;
import io.atomix.primitive.partition.PartitionGroups;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
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
  private WrappedPartitionGroup systemGroup;
  private WrappedPartitionGroup defaultGroup;
  private final Map<String, WrappedPartitionGroup> groups = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  @SuppressWarnings("unchecked")
  public DefaultPartitionService(
      ClusterService clusterService,
      ClusterMessagingService messagingService,
      PrimitiveTypeRegistry primitiveTypeRegistry,
      ManagedPartitionGroup systemGroup,
      Collection<ManagedPartitionGroup> groups) {
    this.clusterService = clusterService;
    this.messagingService = messagingService;
    this.primitiveTypeRegistry = primitiveTypeRegistry;
    this.systemGroup = systemGroup != null ? new WrappedPartitionGroup(systemGroup, true) : null;
    groups.forEach(group -> {
      WrappedPartitionGroup wrappedGroup = new WrappedPartitionGroup(group, true);
      this.groups.put(group.name(), wrappedGroup);
      if (defaultGroup == null) {
        defaultGroup = wrappedGroup;
      }
    });

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
  public PartitionGroup getSystemPartitionGroup() {
    return systemGroup;
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getDefaultPartitionGroup() {
    return defaultGroup;
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getPartitionGroup(String name) {
    return groups.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup> getPartitionGroups() {
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
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> bootstrap(Node node) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    messagingService.<NodeId, PartitionGroupInfo>send(
        BOOTSTRAP_SUBJECT,
        clusterService.getLocalNode().id(),
        serializer::encode,
        serializer::decode,
        node.id())
        .whenComplete((info, error) -> {
          if (error == null) {
            if (systemGroup != null && info.systemGroup != null &&
                (!systemGroup.name().equals(info.systemGroup.getName()) || !systemGroup.type().equals(info.systemGroup.getType()))) {
              future.completeExceptionally(new ConfigurationException("Duplicate system group detected"));
            } else if (systemGroup == null && info.systemGroup != null) {
              systemGroup = new WrappedPartitionGroup(PartitionGroups.createGroup(info.systemGroup), false);
            }

            for (PartitionGroupConfig groupConfig : info.groups) {
              ManagedPartitionGroup group = groups.get(groupConfig.getName());
              if (group == null) {
                WrappedPartitionGroup wrappedGroup = new WrappedPartitionGroup(PartitionGroups.createGroup(groupConfig), false);
                groups.put(groupConfig.getName(), wrappedGroup);
                if (defaultGroup == null) {
                  defaultGroup = wrappedGroup;
                }
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
        .thenCompose(v -> {
          PartitionManagementService managementService = new DefaultPartitionManagementService(
              clusterService,
              messagingService,
              primitiveTypeRegistry,
              new HashBasedPrimaryElectionService(clusterService, messagingService),
              new DefaultSessionIdService());
          if (systemGroup.isMember()) {
            return systemGroup.join(managementService);
          } else {
            return systemGroup.connect(managementService);
          }
        })
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
              .map(group -> {
                if (group.isMember()) {
                  return group.join(managementService);
                } else {
                  return group.connect(managementService);
                }
              })
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

  private static class WrappedPartitionGroup implements ManagedPartitionGroup {
    private final ManagedPartitionGroup group;
    private final boolean member;

    public WrappedPartitionGroup(ManagedPartitionGroup group, boolean member) {
      this.group = group;
      this.member = member;
    }

    public boolean isMember() {
      return member;
    }

    @Override
    public PartitionGroupConfig config() {
      return group.config();
    }

    @Override
    public String name() {
      return group.name();
    }

    @Override
    public PrimitiveProtocol.Type type() {
      return group.type();
    }

    @Override
    public PrimitiveProtocol newProtocol() {
      return group.newProtocol();
    }

    @Override
    public Partition getPartition(PartitionId partitionId) {
      return group.getPartition(partitionId);
    }

    @Override
    public Collection<Partition> getPartitions() {
      return group.getPartitions();
    }

    @Override
    public List<PartitionId> getPartitionIds() {
      return group.getPartitionIds();
    }

    @Override
    public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
      return group.join(managementService);
    }

    @Override
    public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
      return group.connect(managementService);
    }

    @Override
    public CompletableFuture<Void> close() {
      return group.close();
    }
  }
}
