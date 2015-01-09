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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MembershipEvent;
import net.kuujo.copycat.cluster.coordinator.*;
import net.kuujo.copycat.cluster.manager.ClusterManager;
import net.kuujo.copycat.cluster.manager.MemberManager;
import net.kuujo.copycat.internal.CopycatStateContext;
import net.kuujo.copycat.internal.DefaultResourceContext;
import net.kuujo.copycat.internal.cluster.*;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default cluster coordinator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultClusterCoordinator implements ClusterCoordinator {
  private final String uri;
  private final ThreadFactory threadFactory = new NamedThreadFactory("copycat-coordinator-%d");
  private final ScheduledExecutorService executor;
  private final CoordinatorConfig config;
  private final DefaultLocalMemberCoordinator localMember;
  final Map<String, AbstractMemberCoordinator> members = new ConcurrentHashMap<>();
  private final CopycatStateContext context;
  private final ClusterManager cluster;
  private final Map<String, ResourceHolder> resources = new ConcurrentHashMap<>();
  private ScheduledFuture<?> gossipTimer;
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultClusterCoordinator(String uri, CoordinatorConfig config) {
    this.uri = uri;
    this.config = config.copy();
    this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);

    // Set up permanent cluster members based on the given cluster configuration.
    this.localMember = new DefaultLocalMemberCoordinator(new MemberInfo(uri, config.getClusterConfig().getMembers().contains(uri) ? Member.Type.ACTIVE : Member.Type.PASSIVE, Member.State.ALIVE), config.getClusterConfig().getProtocol(), Executors.newSingleThreadExecutor(threadFactory));
    this.members.put(uri, localMember);
    for (String member : config.getClusterConfig().getMembers()) {
      if (!this.members.containsKey(member)) {
        this.members.put(member, new DefaultRemoteMemberCoordinator(new MemberInfo(member, Member.Type.ACTIVE, Member.State.ALIVE), config.getClusterConfig().getProtocol(), Executors.newSingleThreadExecutor(threadFactory)));
      }
    }

    // Set up the global Raft state context and cluster.
    CoordinatedResourceConfig resourceConfig = new CoordinatedResourceConfig()
      .withElectionTimeout(config.getClusterConfig().getElectionTimeout())
      .withHeartbeatInterval(config.getClusterConfig().getHeartbeatInterval())
      .withReplicas(config.getClusterConfig().getMembers())
      .withLog(new BufferedLog());
    this.context = new CopycatStateContext("copycat", uri, resourceConfig);
    this.cluster = new CoordinatorCluster(0, "copycat-cluster", this, context, new ResourceRouter(new KryoSerializer()), new KryoSerializer());
    createResources();
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Handles a membership change event.
   */
  private synchronized void handleMembershipEvent(MembershipEvent event) {
    if (event.type() == MembershipEvent.Type.JOIN && !members.containsKey(event.member().uri())) {
      MemberCoordinator coordinator = ((CoordinatedMember) event.member()).coordinator();
      members.put(coordinator.uri(), (AbstractMemberCoordinator) coordinator);
    } else if (event.type() == MembershipEvent.Type.LEAVE) {
      members.remove(event.member().uri());
    }
  }

  @Override
  public CoordinatorConfig config() {
    return config;
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

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T extends Resource<T>> T getResource(String name) {
    ResourceHolder resource = resources.get(name);
    if (resource == null) {
      throw new ConfigurationException("Invalid resource " + name);
    }
    return (T) resource.resource;
  }

  /**
   * Acquires a resource.
   *
   * @param name The resource name.
   * @return A completable future to be completed once the resource has been acquired.
   */
  public synchronized CompletableFuture<Void> acquireResource(String name) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      if (!resource.config.getReplicas().contains(uri) && resource.state.isClosed()) {
        return resource.cluster.open().thenCompose(v -> resource.state.open());
      }
      return CompletableFuture.completedFuture(null);
    }
    return Futures.exceptionalFuture(new IllegalStateException("Invalid resource " + name));
  }

  /**
   * Releases a resource.
   *
   * @param name The resource name.
   * @return A completable future to be completed once the resource has been released.
   */
  public synchronized CompletableFuture<Void> releaseResource(String name) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      if (!resource.config.getReplicas().contains(uri) && resource.state.isOpen()) {
        return resource.state.close().thenCompose(v -> resource.cluster.close());
      }
      return CompletableFuture.completedFuture(null);
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
      CopycatStateContext state = new CopycatStateContext(name, uri, config);
      ClusterManager cluster = new CoordinatedCluster(name.hashCode(), String.format("copycat-cluster-%s", name), this, state, new ResourceRouter(config.getSerializer()), config.getSerializer());
      ResourceContext context = new DefaultResourceContext(name, config, cluster, state, this);
      resources.put(name, new ResourceHolder(config.getResourceFactory().apply(context), config, cluster, state, context));
    }
  }

  /**
   * Opens all cluster resources.
   */
  private CompletableFuture<Void> openResources() {
    List<CompletableFuture<ResourceContext>> futures = new ArrayList<>(resources.size());
    for (ResourceHolder resource : resources.values()) {
      if (resource.config.getReplicas().contains(uri)) {
        futures.add(resource.cluster.open().thenCompose(v -> resource.state.open()).thenApply(v -> null));
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
      futures.add(resource.state.close().thenCompose(v -> resource.cluster.close()));
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
      .thenRun(() -> open.set(true))
      .thenCompose(v -> openResources())
      .thenRun(() -> cluster.addMembershipListener(this::handleMembershipEvent))
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
      cluster.removeMembershipListener(this::handleMembershipEvent);
      return closeResources()
        .thenRun(() -> {
          if (gossipTimer != null) {
            gossipTimer.cancel(false);
            gossipTimer = null;
          }
        })
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
    private static final int PROTOCOL_ID = 1;
    private final Serializer serializer;

    private ResourceRouter(Serializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public void createRoutes(ClusterManager cluster, RaftProtocol protocol) {
      cluster.member().registerHandler(Topics.SYNC, PROTOCOL_ID, protocol::sync, serializer);
      cluster.member().registerHandler(Topics.PING, PROTOCOL_ID, protocol::ping, serializer);
      cluster.member().registerHandler(Topics.POLL, PROTOCOL_ID, protocol::poll, serializer);
      cluster.member().registerHandler(Topics.APPEND, PROTOCOL_ID, protocol::append, serializer);
      cluster.member().registerHandler(Topics.QUERY, PROTOCOL_ID, protocol::query, serializer);
      cluster.member().registerHandler(Topics.COMMIT, PROTOCOL_ID, protocol::commit, serializer);
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
    private <T extends Request, U extends Response> CompletableFuture<U> handleOutboundRequest(String topic, T request, ClusterManager cluster) {
      MemberManager member = cluster.member(request.uri());
      if (member != null) {
        return member.send(topic, PROTOCOL_ID, request, serializer);
      }
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException(String.format("Invalid URI %s", request.uri())));
      return future;
    }

    @Override
    public void destroyRoutes(ClusterManager cluster, RaftProtocol protocol) {
      cluster.member().unregisterHandler(Topics.SYNC, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.PING, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.POLL, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.APPEND, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.QUERY, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.COMMIT, PROTOCOL_ID);
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
    private final ClusterManager cluster;
    private final CopycatStateContext state;
    private final ResourceContext context;

    private ResourceHolder(Resource resource, CoordinatedResourceConfig config, ClusterManager cluster, CopycatStateContext state, ResourceContext context) {
      this.resource = resource;
      this.config = config;
      this.cluster = cluster;
      this.state = state;
      this.context = context;
    }
  }

}
