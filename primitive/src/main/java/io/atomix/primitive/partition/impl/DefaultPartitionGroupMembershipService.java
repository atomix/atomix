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
package io.atomix.primitive.partition.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.messaging.MessagingException;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionGroupMembershipService;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupFactory;
import io.atomix.primitive.partition.PartitionGroupMembership;
import io.atomix.primitive.partition.PartitionGroupMembershipEvent;
import io.atomix.primitive.partition.PartitionGroupMembershipEventListener;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionGroups;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.atomix.primitive.partition.PartitionGroupMembershipEvent.Type.MEMBERS_CHANGED;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default partition group membership service.
 */
public class DefaultPartitionGroupMembershipService
    extends AbstractListenerManager<PartitionGroupMembershipEvent, PartitionGroupMembershipEventListener>
    implements ManagedPartitionGroupMembershipService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionGroupMembershipService.class);
  private static final String BOOTSTRAP_SUBJECT = "partition-group-bootstrap";

  private final ClusterMembershipService membershipService;
  private final ClusterMessagingService messagingService;
  private final Serializer serializer;
  private volatile PartitionGroupMembership systemGroup;
  private final Map<String, PartitionGroupMembership> groups = Maps.newConcurrentMap();
  private final ClusterMembershipEventListener membershipEventListener = this::handleMembershipChange;
  private final AtomicBoolean started = new AtomicBoolean();
  private volatile ThreadContext threadContext;

  @SuppressWarnings("unchecked")
  public DefaultPartitionGroupMembershipService(
      ClusterMembershipService membershipService,
      ClusterMessagingService messagingService,
      ManagedPartitionGroup systemGroup,
      Collection<ManagedPartitionGroup> groups) {
    this.membershipService = membershipService;
    this.messagingService = messagingService;
    this.systemGroup = systemGroup != null
        ? new PartitionGroupMembership(
        systemGroup.name(),
        systemGroup.config(),
        ImmutableSet.of(membershipService.getLocalMember().id()), true) : null;
    groups.forEach(group -> {
      this.groups.put(group.name(), new PartitionGroupMembership(
          group.name(),
          group.config(),
          ImmutableSet.of(membershipService.getLocalMember().id()), false));
    });

    KryoNamespace.Builder builder = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(MemberId.class)
        .register(MemberId.Type.class)
        .register(PartitionGroupMembership.class)
        .register(PartitionGroupInfo.class)
        .register(PartitionGroupConfig.class)
        .register(MemberGroupStrategy.class);
    for (PartitionGroupFactory factory : PartitionGroups.getGroupFactories()) {
      builder.register(factory.configClass());
    }
    serializer = Serializer.using(builder.build());
  }

  @Override
  public PartitionGroupMembership getSystemMembership() {
    return systemGroup;
  }

  @Override
  public PartitionGroupMembership getMembership(String group) {
    PartitionGroupMembership membership = groups.get(group);
    if (membership != null) {
      return membership;
    }
    return systemGroup.group().equals(group) ? systemGroup : null;
  }

  @Override
  public Collection<PartitionGroupMembership> getMemberships() {
    return groups.values();
  }

  /**
   * Handles a cluster membership change.
   */
  private void handleMembershipChange(ClusterMembershipEvent event) {
    if (event.type() == ClusterMembershipEvent.Type.MEMBER_ADDED) {
      bootstrap(event.subject());
    } else if (event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED) {
      threadContext.execute(() -> {
        PartitionGroupMembership systemGroup = this.systemGroup;
        if (systemGroup != null && systemGroup.members().contains(event.subject().id())) {
          Set<MemberId> newMembers = Sets.newHashSet(systemGroup.members());
          newMembers.remove(event.subject().id());
          PartitionGroupMembership newMembership = new PartitionGroupMembership(systemGroup.group(), systemGroup.config(), ImmutableSet.copyOf(newMembers), true);
          this.systemGroup = newMembership;
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
        }

        groups.values().forEach(group -> {
          if (group.members().contains(event.subject().id())) {
            Set<MemberId> newMembers = Sets.newHashSet(group.members());
            newMembers.remove(event.subject().id());
            PartitionGroupMembership newMembership = new PartitionGroupMembership(group.group(), group.config(), ImmutableSet.copyOf(newMembers), false);
            groups.put(group.group(), newMembership);
            post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
          }
        });
      });
    }
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
    return bootstrap(member, new CompletableFuture<>());
  }

  /**
   * Bootstraps the service from the given node.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> bootstrap(Member member, CompletableFuture<Void> future) {
    LOGGER.debug("{} - Bootstrapping from member {}", membershipService.getLocalMember().id(), member);
    messagingService.<PartitionGroupInfo, PartitionGroupInfo>send(
        BOOTSTRAP_SUBJECT,
        new PartitionGroupInfo(membershipService.getLocalMember().id(), systemGroup, Lists.newArrayList(groups.values())),
        serializer::encode,
        serializer::decode,
        member.id())
        .whenCompleteAsync((info, error) -> {
          if (error == null) {
            try {
              updatePartitionGroups(info);
              future.complete(null);
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          } else {
            error = Throwables.getRootCause(error);
            if (error instanceof MessagingException.NoRemoteHandler || error instanceof TimeoutException) {
              threadContext.schedule(Duration.ofSeconds(1), () -> bootstrap(member, future));
            } else {
              future.complete(null);
            }
          }
        }, threadContext);
    return future;
  }

  private void updatePartitionGroups(PartitionGroupInfo info) {
    if (systemGroup == null && info.systemGroup != null) {
      systemGroup = info.systemGroup;
      post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, systemGroup));
      LOGGER.debug("{} - Bootstrapped system group {} from {}", membershipService.getLocalMember().id(), systemGroup, info.memberId);
    } else if (systemGroup != null && info.systemGroup != null) {
      if ((!systemGroup.group().equals(info.systemGroup.group()) || !systemGroup.config().getType().equals(info.systemGroup.config().getType()))) {
        throw new ConfigurationException("Duplicate system group detected");
      } else {
        Set<MemberId> newMembers = Stream.concat(systemGroup.members().stream(), info.systemGroup.members().stream())
            .filter(memberId -> membershipService.getMember(memberId) != null)
            .collect(Collectors.toSet());
        if (!Sets.difference(newMembers, systemGroup.members()).isEmpty()) {
          systemGroup = new PartitionGroupMembership(systemGroup.group(), systemGroup.config(), ImmutableSet.copyOf(newMembers), true);
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, systemGroup));
          LOGGER.debug("{} - Updated system group {} from {}", membershipService.getLocalMember().id(), systemGroup, info.memberId);
        }
      }
    }

    for (PartitionGroupMembership newMembership : info.groups) {
      PartitionGroupMembership oldMembership = groups.get(newMembership.group());
      if (oldMembership == null) {
        groups.put(newMembership.group(), newMembership);
        post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
        LOGGER.debug("{} - Bootstrapped partition group {} from {}", membershipService.getLocalMember().id(), newMembership, info.memberId);
      } else if ((!oldMembership.group().equals(newMembership.group()) || !oldMembership.config().getType().equals(newMembership.config().getType()))) {
        throw new ConfigurationException("Duplicate partition group " + newMembership.group() + " detected");
      } else {
        Set<MemberId> newMembers = Stream.concat(oldMembership.members().stream(), newMembership.members().stream())
            .filter(memberId -> membershipService.getMember(memberId) != null)
            .collect(Collectors.toSet());
        if (!Sets.difference(newMembers, oldMembership.members()).isEmpty()) {
          PartitionGroupMembership newGroup = new PartitionGroupMembership(oldMembership.group(), oldMembership.config(), ImmutableSet.copyOf(newMembers), false);
          groups.put(oldMembership.group(), newGroup);
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newGroup));
          LOGGER.debug("{} - Updated partition group {} from {}", membershipService.getLocalMember().id(), newGroup, info.memberId);
        }
      }
    }
  }

  private PartitionGroupInfo handleBootstrap(PartitionGroupInfo info) {
    try {
      updatePartitionGroups(info);
    } catch (Exception e) {
      // Log the exception
      LOGGER.warn("{}", e.getMessage());
    }
    return new PartitionGroupInfo(membershipService.getLocalMember().id(), systemGroup, Lists.newArrayList(groups.values()));
  }

  @Override
  public CompletableFuture<PartitionGroupMembershipService> start() {
    threadContext = new SingleThreadContext(namedThreads("atomix-partition-group-membership-service-%d", LOGGER));
    membershipService.addListener(membershipEventListener);
    messagingService.subscribe(BOOTSTRAP_SUBJECT, serializer::decode, this::handleBootstrap, serializer::encode, threadContext);
    return bootstrap().thenApply(v -> {
      LOGGER.info("Started");
      started.set(true);
      return this;
    });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> stop() {
    membershipService.removeListener(membershipEventListener);
    messagingService.unsubscribe(BOOTSTRAP_SUBJECT);
    ThreadContext threadContext = this.threadContext;
    if (threadContext != null) {
      threadContext.close();
    }
    LOGGER.info("Stopped");
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }

  private static class PartitionGroupInfo {
    private final MemberId memberId;
    private final PartitionGroupMembership systemGroup;
    private final Collection<PartitionGroupMembership> groups;

    PartitionGroupInfo(MemberId memberId, PartitionGroupMembership systemGroup, Collection<PartitionGroupMembership> groups) {
      this.memberId = memberId;
      this.systemGroup = systemGroup;
      this.groups = groups;
    }
  }
}
