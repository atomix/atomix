/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.RaftContext;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.cluster.CoordinatedCluster;
import net.kuujo.copycat.internal.cluster.GlobalCluster;
import net.kuujo.copycat.internal.cluster.Router;
import net.kuujo.copycat.internal.cluster.Topics;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.LogFactory;
import net.kuujo.copycat.spi.Protocol;
import net.kuujo.copycat.spi.ResourceFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCoordinator implements Coordinator {
  private final GlobalCluster cluster;
  private final DefaultCopycatContext context;
  private final ExecutionContext executor;
  private final Map<String, ResourceInfo> resources = new HashMap<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, CoordinatedCluster> clusters = new HashMap<>();
  private final Router router = new Router() {
    @Override
    public void createRoutes(Cluster cluster, RaftContext context) {
      cluster.localMember().handler(Topics.CONFIGURE, context::configure);
      cluster.localMember().handler(Topics.PING, context::ping);
      cluster.localMember().handler(Topics.POLL, context::poll);
      cluster.localMember().handler(Topics.SYNC, context::sync);
      cluster.localMember().handler(Topics.COMMIT, context::commit);
    }
    @Override
    public void destroyRoutes(Cluster cluster, RaftContext context) {
      cluster.localMember().handler(Topics.CONFIGURE, null);
      cluster.localMember().handler(Topics.PING, null);
      cluster.localMember().handler(Topics.POLL, null);
      cluster.localMember().handler(Topics.SYNC, null);
      cluster.localMember().handler(Topics.COMMIT, null);
    }
  };

  public DefaultCoordinator(ClusterConfig config, Protocol protocol, Log log, ExecutionContext executor) {
    ClusterContext clusterContext = new ClusterContext(config);
    this.context = new DefaultCopycatContext(log, clusterContext, executor);
    this.cluster = new GlobalCluster(protocol, context, router, executor);
    this.executor = executor;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Resource> CompletableFuture<T> createResource(String name, LogFactory logFactory, ResourceFactory<T> resourceFactory) {
    CompletableFuture<T> future = new CompletableFuture<>();
    JoinEntry entry = new JoinEntry();
    entry.resource = name;
    entry.owner = cluster.localMember().uri();
    CommitRequest request = CommitRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withEntry(entry)
      .build();
    context.commit(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          CoordinatedCluster cluster = clusters.get(name);
          if (cluster == null) {
            RaftContext context = new DefaultCopycatContext(logFactory.createLog(name), new ClusterContext(this.cluster.localMember().uri(), response.result()), ExecutionContext.create());
            cluster = new CoordinatedCluster(this.cluster, context, new ResourceRouter(name));
            clusters.put(name, cluster);
          }
          future.complete(resourceFactory.createResource(name, this, cluster, cluster.state()));
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> deleteResource(String name) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an entry to the coordinator.
   */
  private Object applyEntry(Object entry) {
    if (entry instanceof JoinEntry) {
      String resource = ((ResourceEntry) entry).resource;
      String owner = ((ResourceEntry) entry).owner;
      ResourceInfo holder = resources.get(resource);
      if (holder != null) {
        holder.members.add(owner);
      } else {
        Set<String> cluster = new HashSet<>(50);
        cluster.add(owner);
        holder = new ResourceInfo(resource, cluster);
        resources.put(resource, holder);
      }
      CoordinatedCluster cluster = clusters.get(resource);
      if (cluster != null) {
        Set<String> members = new HashSet<>(holder.members);
        members.remove(cluster.localMember().uri());
        cluster.configure(new ClusterConfig()
          .withLocalMember(cluster.localMember().uri())
          .withRemoteMembers(members));
      }
      return holder.members;
    } else if (entry instanceof LeaveEntry) {
      String resource = ((ResourceEntry) entry).resource;
      ResourceInfo info = resources.remove(resource);
      return info != null;
    }
    throw new IllegalStateException("Invalid entry");
  }

  @Override
  public CompletableFuture<Void> open() {
    return CompletableFuture.allOf(cluster.open(), context.open()).thenRun(() -> {
      context.applyHandler(this::applyEntry);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.anyOf(cluster.close(), context.close()).thenRun(() -> {
      context.applyHandler(null);
    });
  }

  /**
   * Coordinator entry.
   */
  private static interface CoordinatorEntry extends Serializable {
  }

  /**
   * Base resource entry.
   */
  private abstract static class ResourceEntry implements CoordinatorEntry {
    protected String resource;
    protected String owner;
  }

  /**
   * Join resource entry.
   */
  private static class JoinEntry extends ResourceEntry {
  }

  /**
   * Leave resource entry.
   */
  private static class LeaveEntry extends ResourceEntry {
  }

  /**
   * Resource info holder.
   */
  private static class ResourceInfo {
    private final String resource;
    private final Set<String> members;
    private ResourceInfo(String resource, Set<String> members) {
      this.resource = resource;
      this.members = members;
    }
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
    public void createRoutes(Cluster cluster, RaftContext context) {
      cluster.localMember().<Request, Response>handler(name, request -> handleInboundRequest(request, context));
      context.configureHandler(request -> handleOutboundRequest(request, cluster));
      context.pingHandler(request -> handleOutboundRequest(request, cluster));
      context.pollHandler(request -> handleOutboundRequest(request, cluster));
      context.syncHandler(request -> handleOutboundRequest(request, cluster));
      context.commitHandler(request -> handleOutboundRequest(request, cluster));
    }

    /**
     * Handles an inbound protocol request.
     */
    @SuppressWarnings("unchecked")
    private <T extends Request, U extends Response> CompletableFuture<U> handleInboundRequest(T request, RaftContext context) {
      if (request instanceof ConfigureRequest) {
        return (CompletableFuture<U>) context.configure((ConfigureRequest) request);
      } else if (request instanceof PingRequest) {
        return (CompletableFuture<U>) context.ping((PingRequest) request);
      } else if (request instanceof PollRequest) {
        return (CompletableFuture<U>) context.poll((PollRequest) request);
      } else if (request instanceof SyncRequest) {
        return (CompletableFuture<U>) context.sync((SyncRequest) request);
      } else if (request instanceof CommitRequest) {
        return (CompletableFuture<U>) context.commit((CommitRequest) request);
      } else {
        CompletableFuture<U> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException(String.format("Invalid request type %s", request.getClass())));
        return future;
      }
    }

    /**
     * Handles an outbound protocol request.
     */
    private <T extends Request, U extends Response> CompletableFuture<U> handleOutboundRequest(T request, Cluster cluster) {
      Member member = cluster.member(request.member());
      if (member != null) {
        return member.send(name, request);
      }
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException(String.format("Invalid URI %s", request.member())));
      return future;
    }

    @Override
    public void destroyRoutes(Cluster cluster, RaftContext context) {
      cluster.localMember().<Request, Response>handler(name, null);
      context.configureHandler(null);
      context.pingHandler(null);
      context.pollHandler(null);
      context.syncHandler(null);
      context.commitHandler(null);
    }
  }

}
