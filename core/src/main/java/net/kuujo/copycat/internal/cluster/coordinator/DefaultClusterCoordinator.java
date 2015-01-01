/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal.cluster.coordinator;

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.ResourcePartitionContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.*;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.internal.DefaultResourceContext;
import net.kuujo.copycat.internal.DefaultResourcePartitionContext;
import net.kuujo.copycat.internal.cluster.CoordinatedCluster;
import net.kuujo.copycat.internal.cluster.Router;
import net.kuujo.copycat.internal.cluster.Topics;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.Response;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default cluster coordinator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultClusterCoordinator extends Observable implements ClusterCoordinator {
  private final String uri;
  private final Executor executor;
  private final CoordinatorConfig config;
  private final DefaultLocalMemberCoordinator localMember;
  private final Map<String, AbstractMemberCoordinator> members = new ConcurrentHashMap<>();
  private final CopycatStateContext context;
  private final Cluster cluster;
  private final Map<String, ResourceHolder> resources = new ConcurrentHashMap<>();
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultClusterCoordinator(String uri, CoordinatorConfig config) {
    this(uri, config, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d")));
  }

  public DefaultClusterCoordinator(String uri, CoordinatorConfig config, Executor executor) {
    this.uri = uri;
    this.config = config.copy();
    this.executor = executor;
    this.localMember = new DefaultLocalMemberCoordinator(uri, config.getClusterConfig().getMembers().contains(uri) ? Member.Type.MEMBER : Member.Type.LISTENER, Member.State.ALIVE, config.getClusterConfig().getProtocol(), executor);
    this.members.put(uri, localMember);
    for (String member : config.getClusterConfig().getMembers()) {
      this.members.put(member, new DefaultRemoteMemberCoordinator(member, Member.Type.MEMBER, Member.State.ALIVE, config.getClusterConfig().getProtocol(), executor));
    }
    CoordinatedResourceConfig resourceConfig = new CoordinatedResourceConfig()
      .withElectionTimeout(config.getClusterConfig().getElectionTimeout())
      .withHeartbeatInterval(config.getClusterConfig().getHeartbeatInterval())
      .withLog(new BufferedLog());
    CoordinatedResourcePartitionConfig partitionConfig = new CoordinatedResourcePartitionConfig()
      .withPartition(1)
      .withReplicas(config.getClusterConfig().getMembers());
    this.context = new CopycatStateContext("copycat", uri, resourceConfig, partitionConfig);
    this.cluster = new CoordinatedCluster(0, this, context, new ResourceRouter("copycat"), Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d")));
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public LocalMemberCoordinator member() {
    return localMember;
  }

  @Override
  public MemberCoordinator member(String uri) {
    return members.get(uri);
  }

  @Override
  public Collection<MemberCoordinator> members() {
    return Collections.unmodifiableCollection(members.values());
  }

  /**
   * Gets a Copycat resource.
   *
   * @param name The resource name.
   * @return A completable future to be completed once the resource has been retrieved.
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> getResource(String name) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      return CompletableFuture.completedFuture((T) resource.resource);
    }
    return Futures.exceptionalFuture(new IllegalStateException("Invalid resource " + name));
  }

  /**
   * Acquires a resource partition.
   *
   * @param name The resource name.
   * @param partition The partition number.
   * @return A completable future to be completed once the partition has been acquired.
   */
  CompletableFuture<Void> acquirePartition(String name, int partition) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      PartitionHolder partitionHolder = resource.partitions.get(partition - 1);
      CompletableFuture<Void> future = new CompletableFuture<>();
      partitionHolder.state.executor().execute(() -> {
        if (!partitionHolder.config.getReplicas().contains(uri) && partitionHolder.state.isClosed()) {
          partitionHolder.cluster.open().whenComplete((r1, e1) -> {
            if (e1 == null) {
              partitionHolder.state.open().whenComplete((r2, e2) -> {
                if (e2 == null) {
                  future.complete(null);
                } else {
                  future.completeExceptionally(e2);
                }
              });
            } else {
              future.completeExceptionally(e1);
            }
          });
        } else {
          future.complete(null);
        }
      });
    }
    return Futures.exceptionalFuture(new IllegalStateException("Invalid resource " + name));
  }

  /**
   * Releases a resource partition.
   *
   * @param name The resource name.
   * @param partition The partition number.
   * @return A completable future to be completed once the partition has been released.
   */
  CompletableFuture<Void> releasePartition(String name, int partition) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      PartitionHolder partitionHolder = resource.partitions.get(partition - 1);
      CompletableFuture<Void> future = new CompletableFuture<>();
      partitionHolder.state.executor().execute(() -> {
        if (!partitionHolder.config.getReplicas().contains(uri) && partitionHolder.state.isOpen()) {
          partitionHolder.state.close().whenComplete((r1, e1) -> {
            if (e1 == null) {
              partitionHolder.cluster.close().whenComplete((r2, e2) -> {
                if (e2 == null) {
                  future.complete(null);
                } else {
                  future.completeExceptionally(e2);
                }
              });
            } else {
              future.completeExceptionally(e1);
            }
          });
        } else {
          future.complete(null);
        }
      });
    }
    return Futures.exceptionalFuture(new IllegalStateException("Invalid resource " + name));
  }

  /**
   * Creates all Copycat resources.
   */
  private void createResources() {
    for (Map.Entry<String, CoordinatedResourceConfig> entry : this.config.getResourceConfigs().entrySet()) {
      String name = entry.getKey();
      CoordinatedResourceConfig config = entry.getValue();

      List<PartitionHolder> partitions = new ArrayList<>(config.getPartitions().size());
      for (CoordinatedResourcePartitionConfig partitionConfig : config.getPartitions()) {
        CopycatStateContext state = new CopycatStateContext(name, uri, config, partitionConfig);
        Cluster cluster = new CoordinatedCluster(name.hashCode(), this, context, new ResourceRouter(name), Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-resource-" + name + "-%d")));
        ResourcePartitionContext context = new DefaultResourcePartitionContext(name, partitionConfig, cluster, state);
        partitions.add(new PartitionHolder(partitionConfig, cluster, state, context));
      }

      Resource resource = config.getResourceFactory().apply(new DefaultResourceContext(name, config, partitions.stream()
        .collect(Collectors.mapping(p -> p.context, Collectors.toList())), this));
      resources.put(name, new ResourceHolder(resource, config, partitions));
    }
  }

  /**
   * Opens all cluster resources.
   */
  private CompletableFuture<Void> openResources() {
    List<CompletableFuture<ResourcePartitionContext>> futures = new ArrayList<>();
    for (ResourceHolder resource : resources.values()) {
      for (PartitionHolder partition : resource.partitions) {
        if (partition.config.getReplicas().contains(uri)) {
          futures.add(partition.cluster.open().thenCompose(v -> partition.context.open()));
        }
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Closes all cluster resources.
   */
  private CompletableFuture<Void> closeResources() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (ResourceHolder resource : resources.values()) {
      for (PartitionHolder partition : resource.partitions) {
        futures.add(partition.context.close().thenCompose(v -> partition.cluster.close()));
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ClusterCoordinator> open() {
    if (open.get()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<MemberCoordinator>[] futures = new CompletableFuture[members.size()];
    int i = 0;
    for (MemberCoordinator member : members.values()) {
      futures[i++] = member.open();
    }
    return CompletableFuture.allOf(futures)
      .thenComposeAsync(v -> cluster.open(), executor)
      .thenComposeAsync(v -> context.open(), executor)
      .thenComposeAsync(v -> openResources(), executor)
      .thenRun(() -> open.set(true))
      .thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      CompletableFuture<Void>[] futures = new CompletableFuture[members.size()];
      int i = 0;
      for (MemberCoordinator member : members.values()) {
        futures[i++] = member.close();
      }
      return closeResources()
        .thenComposeAsync(v -> context.close(), executor)
        .thenComposeAsync(v -> cluster.close(), executor)
        .thenComposeAsync(v -> CompletableFuture.allOf(futures));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getCanonicalName(), members.values());
  }

  /**
   * Resource router.
   */
  private static class ResourceRouter implements Router {
    private final String name;

    private ResourceRouter(String name) {
      this.name = name;
    }

    @Override
    public void createRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.member().registerHandler(Topics.SYNC, protocol::sync);
      cluster.member().registerHandler(Topics.PING, protocol::ping);
      cluster.member().registerHandler(Topics.POLL, protocol::poll);
      cluster.member().registerHandler(Topics.APPEND, protocol::append);
      cluster.member().registerHandler(Topics.QUERY, protocol::query);
      cluster.member().registerHandler(Topics.COMMIT, protocol::commit);
      protocol.pingHandler(request -> handleOutboundRequest(Topics.SYNC, request, cluster));
      protocol.pingHandler(request -> handleOutboundRequest(Topics.PING, request, cluster));
      protocol.pollHandler(request -> handleOutboundRequest(Topics.POLL, request, cluster));
      protocol.appendHandler(request -> handleOutboundRequest(Topics.APPEND, request, cluster));
      protocol.queryHandler(request -> handleOutboundRequest(Topics.QUERY, request, cluster));
      protocol.commitHandler(request -> handleOutboundRequest(Topics.COMMIT, request, cluster));
    }

    /**
     * Handles an outbound protocol request.
     */
    private <T extends Request, U extends Response> CompletableFuture<U> handleOutboundRequest(String topic, T request,
                                                                                               Cluster cluster) {
      Member member = cluster.member(request.uri());
      if (member != null) {
        return member.send(topic, request);
      }
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException(String.format("Invalid URI %s", request.uri())));
      return future;
    }

    @Override
    public void destroyRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.member().unregisterHandler(name);
      protocol.pingHandler(null);
      protocol.pollHandler(null);
      protocol.appendHandler(null);
      protocol.queryHandler(null);
      protocol.commitHandler(null);
    }
  }

  /**
   * Container for all resource related types.
   */
  @SuppressWarnings("rawtypes")
  private static class ResourceHolder {
    private final Resource resource;
    private final CoordinatedResourceConfig config;
    private final List<PartitionHolder> partitions;

    private ResourceHolder(Resource resource, CoordinatedResourceConfig config, List<PartitionHolder> partitions) {
      this.resource = resource;
      this.config = config;
      this.partitions = partitions;
    }
  }

  /**
   * Container for resource partitions.
   */
  private static class PartitionHolder {
    private final CoordinatedResourcePartitionConfig config;
    private final Cluster cluster;
    private final CopycatStateContext state;
    private final ResourcePartitionContext context;

    private PartitionHolder(CoordinatedResourcePartitionConfig config, Cluster cluster, CopycatStateContext state, ResourcePartitionContext context) {
      this.config = config;
      this.cluster = cluster;
      this.state = state;
      this.context = context;
    }
  }

}
