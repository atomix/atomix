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
package net.kuujo.copycat.cluster.internal.coordinator;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MembershipEvent;
import net.kuujo.copycat.cluster.internal.*;
import net.kuujo.copycat.cluster.internal.manager.ClusterManager;
import net.kuujo.copycat.cluster.internal.manager.MemberManager;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.protocol.rpc.Request;
import net.kuujo.copycat.protocol.rpc.Response;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.internal.CopycatStateContext;
import net.kuujo.copycat.resource.internal.DefaultResourceContext;
import net.kuujo.copycat.resource.internal.ResourceContext;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

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
  private volatile boolean open;

  public DefaultClusterCoordinator(String uri, CoordinatorConfig config) {
    this.uri = uri;
    this.config = config.copy();
    this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);

    // Set up permanent cluster members based on the given cluster configuration.
    this.localMember = new DefaultLocalMemberCoordinator(new MemberInfo(uri, config.getClusterConfig().getMembers().contains(uri) ? Member.Type.ACTIVE : Member.Type.PASSIVE, Member.State.ALIVE), config.getClusterConfig().getProtocol(), Executors.newSingleThreadExecutor(threadFactory));
    this.members.put(uri, localMember);
    for (String member : config.getClusterConfig().getMembers()) {
      if (!this.members.containsKey(member)) {
        this.members.put(member, new DefaultRemoteMemberCoordinator(new MemberInfo(member, Member.Type.ACTIVE, Member.State.ALIVE), config.getClusterConfig().getProtocol(), Executors.newSingleThreadScheduledExecutor(threadFactory)));
      }
    }

    // Set up the global Raft state context and cluster.
    CoordinatedResourceConfig resourceConfig = new CoordinatedResourceConfig()
      .withElectionTimeout(config.getClusterConfig().getElectionTimeout())
      .withHeartbeatInterval(config.getClusterConfig().getHeartbeatInterval())
      .withReplicas(config.getClusterConfig().getMembers())
      .withLog(new BufferedLog());
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-coordinator"));
    this.context = new CopycatStateContext(config.getName(), uri, resourceConfig, executor);
    this.cluster = new CoordinatorCluster(0, this, context, new ResourceRouter(executor), new KryoSerializer(), executor, config.getExecutor());
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Handles a membership change event.
   */
  private void handleMembershipEvent(MembershipEvent event) {
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
  public <T extends Resource<T>> T getResource(String name) {
    return getResource(name, new CoordinatedResourceConfig());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource<T>> T getResource(String name, CoordinatedResourceConfig config) {
    ResourceHolder resource = resources.computeIfAbsent(name, n -> {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"));
      CopycatStateContext state = new CopycatStateContext(name, uri, config, executor);
      ClusterManager cluster = new CoordinatedCluster(name.hashCode(), this, state, new ResourceRouter(executor), config.getSerializer(), executor, config.getExecutor());
      ResourceContext context = new DefaultResourceContext(name, config, cluster, state, this);
      try {
        return new ResourceHolder(config.getResourceType().getConstructor(ResourceContext.class).newInstance(context), cluster, state);
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new ConfigurationException("Failed to instantiate resource", e);
      }
    });
    return (T) resource.resource;
  }

  /**
   * Acquires a resource.
   *
   * @param name The resource name.
   * @return A completable future to be completed once the resource has been acquired.
   */
  public CompletableFuture<Void> acquireResource(String name) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      if (resource.cluster.isClosed()) {
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
  public CompletableFuture<Void> releaseResource(String name) {
    Assert.state(isOpen(), "coordinator not open");
    ResourceHolder resource = resources.get(name);
    if (resource != null) {
      if (resource.cluster.isOpen()) {
        return resource.state.close().thenCompose(v -> resource.cluster.close());
      }
      return CompletableFuture.completedFuture(null);
    }
    return Futures.exceptionalFuture(new IllegalStateException("Invalid resource " + name));
  }

  /**
   * Closes all cluster resources.
   */
  private synchronized CompletableFuture<Void> closeResources() {
    List<CompletableFuture<Void>> futures = new ArrayList<>(resources.size());
    for (ResourceHolder resource : resources.values()) {
      if (resource.cluster.isOpen()) {
        futures.add(resource.state.close()
          .thenCompose(v -> resource.cluster.close())
          .thenRun(() -> resource.state.executor().shutdown()));
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<ClusterCoordinator> open() {
    if (open) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<MemberCoordinator>[] futures = new CompletableFuture[members.size()];
    int i = 0;
    for (MemberCoordinator member : members.values()) {
      futures[i++] = member.open();
    }
    return CompletableFuture.allOf(futures)
      .thenRun(() -> cluster.addMembershipListener(this::handleMembershipEvent))
      .thenComposeAsync(v -> cluster.open(), executor)
      .thenComposeAsync(v -> context.open(), executor)
      .thenRun(() -> open = true)
      .thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<Void> close() {
    if (!open) {
      return CompletableFuture.completedFuture(null);
    }
    
    CompletableFuture<Void>[] futures = new CompletableFuture[members.size()];
    int i = 0;
    for (MemberCoordinator member : members.values()) {
      futures[i++] = member.close();
    }
    cluster.removeMembershipListener(this::handleMembershipEvent);
    return closeResources()
      .thenComposeAsync(v -> context.close(), executor)
      .thenComposeAsync(v -> cluster.close(), executor)
      .thenComposeAsync(v -> CompletableFuture.allOf(futures));
  }

  @Override
  public boolean isClosed() {
    return !open;
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
    private final Serializer serializer = new KryoSerializer();
    private final Executor executor;

    private ResourceRouter(Executor executor) {
      this.executor = executor;
    }

    @Override
    public void createRoutes(ClusterManager cluster, RaftProtocol protocol) {
      cluster.member().registerHandler(Topics.SYNC, PROTOCOL_ID, protocol::sync, serializer, executor);
      cluster.member().registerHandler(Topics.POLL, PROTOCOL_ID, protocol::poll, serializer, executor);
      cluster.member().registerHandler(Topics.APPEND, PROTOCOL_ID, protocol::append, serializer, executor);
      cluster.member().registerHandler(Topics.QUERY, PROTOCOL_ID, protocol::query, serializer, executor);
      cluster.member().registerHandler(Topics.COMMIT, PROTOCOL_ID, protocol::commit, serializer, executor);
      protocol.syncHandler(request -> handleOutboundRequest(Topics.SYNC, request, cluster));
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
        return member.send(topic, PROTOCOL_ID, request, serializer, executor);
      }
      return Futures.exceptionalFuture(new IllegalStateException(String.format("Invalid member URI %s", request.uri())));
    }

    @Override
    public void destroyRoutes(ClusterManager cluster, RaftProtocol protocol) {
      cluster.member().unregisterHandler(Topics.SYNC, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.POLL, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.APPEND, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.QUERY, PROTOCOL_ID);
      cluster.member().unregisterHandler(Topics.COMMIT, PROTOCOL_ID);
      protocol.syncHandler(null);
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
    private final ClusterManager cluster;
    private final CopycatStateContext state;

    private ResourceHolder(Resource resource, ClusterManager cluster, CopycatStateContext state) {
      this.resource = resource;
      this.cluster = cluster;
      this.state = state;
    }
  }

}
