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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.MemberGroupStrategy;
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
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
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
import java.util.stream.Stream;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default partition service.
 */
public class DefaultPartitionService implements ManagedPartitionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);
  private static final String BOOTSTRAP_SUBJECT = "partition-bootstrap";

  private final ClusterMembershipService membershipService;
  private final ClusterMessagingService messagingService;
  private final PrimitiveTypeRegistry primitiveTypeRegistry;
  private final Serializer serializer;
  private WrappedPartitionGroup systemGroup;
  private WrappedPartitionGroup defaultGroup;
  private final Map<String, WrappedPartitionGroup<?>> groups = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();
  private ThreadContext threadContext;

  @SuppressWarnings("unchecked")
  public DefaultPartitionService(
      ClusterMembershipService membershipService,
      ClusterMessagingService messagingService,
      PrimitiveTypeRegistry primitiveTypeRegistry,
      ManagedPartitionGroup systemGroup,
      Collection<ManagedPartitionGroup> groups) {
    this.membershipService = membershipService;
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
        .register(MemberId.class)
        .register(PartitionGroupInfo.class)
        .register(PartitionGroupConfig.class)
        .register(MemberGroupStrategy.class);
    for (PartitionGroupFactory factory : PartitionGroups.getGroupFactories()) {
      builder.register(factory.configClass());
    }
    serializer = Serializer.using(builder.build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getSystemPartitionGroup() {
    return systemGroup.group;
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getPartitionGroup(String name) {
    WrappedPartitionGroup group = groups.get(name);
    if (group != null) {
      return group;
    }
    if (systemGroup != null && systemGroup.name().equals(name)) {
      return systemGroup;
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup> getPartitionGroups() {
    return groups.values()
        .stream()
        .map(group -> group.group)
        .collect(Collectors.toList());
  }

  /**
   * Bootstraps the service.
   */
  private CompletableFuture<Void> bootstrap() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Futures.allOf(membershipService.getMembers().stream()
        .filter(node -> !node.id().equals(membershipService.getLocalMember().id()))
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
  private CompletableFuture<Void> bootstrap(Member member) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    messagingService.<MemberId, PartitionGroupInfo>send(
        BOOTSTRAP_SUBJECT,
        membershipService.getLocalMember().id(),
        serializer::encode,
        serializer::decode,
        member.id())
        .whenComplete((info, error) -> {
          if (error == null) {
            if (systemGroup != null && info.systemGroup != null &&
                (!systemGroup.name().equals(info.systemGroup.getName()) || !systemGroup.protocol().equals(info.systemGroup.getType()))) {
              future.completeExceptionally(new ConfigurationException("Duplicate system group detected"));
              return;
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
              } else if (!group.protocol().equals(groupConfig.getType())) {
                future.completeExceptionally(new ConfigurationException("Duplicate partition group " + groupConfig.getName() + " detected"));
                return;
              }
            }
            future.complete(null);
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  private PartitionGroupInfo handleBootstrap(MemberId memberId) {
    return new PartitionGroupInfo(
        systemGroup != null ? systemGroup.config() : null,
        groups.values()
            .stream()
            .map(group -> group.config())
            .collect(Collectors.toList()));
  }

  @Override
  public CompletableFuture<PartitionService> start() {
    threadContext = new SingleThreadContext(namedThreads("atomix-partition-service-%d", LOGGER));
    messagingService.subscribe(BOOTSTRAP_SUBJECT, serializer::decode, this::handleBootstrap, serializer::encode, threadContext);
    return bootstrap()
        .thenCompose(v -> {
          PartitionManagementService managementService = new DefaultPartitionManagementService(
              membershipService,
              messagingService,
              primitiveTypeRegistry,
              new HashBasedPrimaryElectionService(membershipService, messagingService),
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
                  membershipService,
                  messagingService,
                  primitiveTypeRegistry,
                  systemElectionService,
                  systemSessionIdService));
        })
        .thenCompose(managementService -> {
          List<CompletableFuture> futures = groups.values().stream()
              .map(group -> {
                if (group.isMember()) {
                  return group.join((PartitionManagementService) managementService);
                } else {
                  return group.connect((PartitionManagementService) managementService);
                }
              })
              .collect(Collectors.toList());
          return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
            LOGGER.info("Started");
            started.set(true);
            return this;
          });
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> stop() {
    messagingService.unsubscribe(BOOTSTRAP_SUBJECT);

    Stream<CompletableFuture<Void>> systemStream = Stream.of(systemGroup != null ? systemGroup.close() : CompletableFuture.completedFuture(null));
    Stream<CompletableFuture<Void>> groupStream = groups.values().stream().map(ManagedPartitionGroup::close);
    List<CompletableFuture<Void>> futures = Stream.concat(systemStream, groupStream).collect(Collectors.toList());

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      ThreadContext threadContext = this.threadContext;
      if (threadContext != null) {
        threadContext.close();
      }
      LOGGER.info("Stopped");
      started.set(false);
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

  private static class WrappedPartitionGroup<P extends Partition> implements ManagedPartitionGroup<P> {
    private final ManagedPartitionGroup<P> group;
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
    public Type type() {
      return group.type();
    }

    @Override
    public PrimitiveProtocol.Type protocol() {
      return group.protocol();
    }

    @Override
    public PrimitiveProtocol newProtocol() {
      return group.newProtocol();
    }

    @Override
    public P getPartition(PartitionId partitionId) {
      return group.getPartition(partitionId);
    }

    @Override
    public Collection<P> getPartitions() {
      return group.getPartitions();
    }

    @Override
    public List<PartitionId> getPartitionIds() {
      return group.getPartitionIds();
    }

    @Override
    public CompletableFuture<ManagedPartitionGroup<P>> join(PartitionManagementService managementService) {
      return group.join(managementService);
    }

    @Override
    public CompletableFuture<ManagedPartitionGroup<P>> connect(PartitionManagementService managementService) {
      return group.connect(managementService);
    }

    @Override
    public CompletableFuture<Void> close() {
      return group.close();
    }
  }
}
