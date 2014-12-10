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
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
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
  private final DefaultCopycatStateContext context;
  private final ExecutionContext executor;
  private final Map<String, ResourceInfo> resources = new HashMap<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, DefaultCopycatStateContext> contexts = new HashMap<>();
  private final Router router = new Router() {
    @Override
    public void createRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.localMember().handler(Topics.CONFIGURE, protocol::configure);
      cluster.localMember().handler(Topics.PING, protocol::ping);
      cluster.localMember().handler(Topics.POLL, protocol::poll);
      cluster.localMember().handler(Topics.SYNC, protocol::sync);
      cluster.localMember().handler(Topics.COMMIT, protocol::commit);
    }
    @Override
    public void destroyRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.localMember().handler(Topics.CONFIGURE, null);
      cluster.localMember().handler(Topics.PING, null);
      cluster.localMember().handler(Topics.POLL, null);
      cluster.localMember().handler(Topics.SYNC, null);
      cluster.localMember().handler(Topics.COMMIT, null);
    }
  };

  public DefaultCoordinator(ClusterConfig config, Protocol protocol, Log log, ExecutionContext executor) {
    this.cluster = new GlobalCluster(config, protocol, router, executor);
    this.context = new DefaultCopycatStateContext(cluster, config, log, executor);
    cluster.setState(context);
    this.executor = executor;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<CopycatContext> createResource(String name, LogFactory logFactory) {
    CompletableFuture<CopycatContext> future = new CompletableFuture<>();
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
          DefaultCopycatStateContext context = contexts.get(name);
          if (context == null) {
            ExecutionContext executor = ExecutionContext.create();
            CoordinatedCluster cluster = new CoordinatedCluster(this.cluster, new ResourceRouter(name), executor);
            context = new DefaultCopycatStateContext(cluster, new ClusterConfig().withLocalMember(this.cluster.localMember().uri()), logFactory.createLog(name), ExecutionContext.create());
            cluster.setState(context);
            contexts.put(name, context);
          }
          future.complete(context);
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
   * Handles a join action.
   */
  private Set<String> join(Long index, JoinEntry entry) {
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
    DefaultCopycatStateContext context = contexts.get(resource);
    if (context != null) {
      Cluster cluster = context.cluster();
      Set<String> members = new HashSet<>(holder.members);
      members.remove(cluster.localMember().uri());
      cluster.configure(new ClusterConfig()
        .withLocalMember(cluster.localMember().uri())
        .withRemoteMembers(members));
    }
    return holder.members;
  }

  /**
   * Handles a leave action.
   */
  private boolean leave(Long index, LeaveEntry entry) {
    String resource = ((ResourceEntry) entry).resource;
    ResourceInfo info = resources.remove(resource);
    return info != null;
  }

  @Override
  public CompletableFuture<Void> open() {
    return CompletableFuture.allOf(cluster.open(), context.open()).thenRun(() -> {
      context.register("join", this::join).register("leave", this::leave);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.anyOf(cluster.close(), context.close()).thenRun(() -> {
      context.unregister("join").unregister("leave");
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
    public void createRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.localMember().<Request, Response>handler(name, request -> handleInboundRequest(request, protocol));
      protocol.configureHandler(request -> handleOutboundRequest(request, cluster));
      protocol.pingHandler(request -> handleOutboundRequest(request, cluster));
      protocol.pollHandler(request -> handleOutboundRequest(request, cluster));
      protocol.syncHandler(request -> handleOutboundRequest(request, cluster));
      protocol.commitHandler(request -> handleOutboundRequest(request, cluster));
    }

    /**
     * Handles an inbound protocol request.
     */
    @SuppressWarnings("unchecked")
    private <T extends Request, U extends Response> CompletableFuture<U> handleInboundRequest(T request, RaftProtocol protocol) {
      if (request instanceof ConfigureRequest) {
        return (CompletableFuture<U>) protocol.configure((ConfigureRequest) request);
      } else if (request instanceof PingRequest) {
        return (CompletableFuture<U>) protocol.ping((PingRequest) request);
      } else if (request instanceof PollRequest) {
        return (CompletableFuture<U>) protocol.poll((PollRequest) request);
      } else if (request instanceof SyncRequest) {
        return (CompletableFuture<U>) protocol.sync((SyncRequest) request);
      } else if (request instanceof CommitRequest) {
        return (CompletableFuture<U>) protocol.commit((CommitRequest) request);
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
    public void destroyRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.localMember().<Request, Response>handler(name, null);
      protocol.configureHandler(null);
      protocol.pingHandler(null);
      protocol.pollHandler(null);
      protocol.syncHandler(null);
      protocol.commitHandler(null);
    }
  }

}
