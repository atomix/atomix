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

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.CopycatCoordinator;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.cluster.*;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.LogFactory;
import net.kuujo.copycat.spi.Protocol;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copycat coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatCoordinator implements CopycatCoordinator {
  private final GlobalCluster cluster;
  private final DefaultCopycatStateContext context;
  private final ExecutionContext executor;
  private final Map<String, ResourceInfo> resources = new HashMap<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, DefaultCopycatStateContext> contexts = new HashMap<>();
  private final Router router = new Router() {
    @Override
    public void createRoutes(Cluster cluster, RaftProtocol protocol) {
      GlobalCluster globalCluster = (GlobalCluster) cluster;
      protocol.pingHandler(request -> handleOutboundRequest(Topics.PING, request, globalCluster));
      protocol.pollHandler(request -> handleOutboundRequest(Topics.POLL, request, globalCluster));
      protocol.appendHandler(request -> handleOutboundRequest(Topics.APPEND, request, globalCluster));
      protocol.appendHandler(request -> handleOutboundRequest(Topics.SYNC, request, globalCluster));
      protocol.commitHandler(request -> handleOutboundRequest(Topics.COMMIT, request, globalCluster));
      globalCluster.localMember().register(Topics.PING, 0, protocol::ping);
      globalCluster.localMember().register(Topics.POLL, 0, protocol::poll);
      globalCluster.localMember().register(Topics.APPEND, 0, protocol::append);
      globalCluster.localMember().register(Topics.SYNC, 0, protocol::sync);
      globalCluster.localMember().register(Topics.COMMIT, 0, protocol::commit);
    }
    private <T extends Request, U extends Response> CompletableFuture<U> handleOutboundRequest(String topic, T request, GlobalCluster cluster) {
      String uri = request.member();
      if (uri == null) {
        CompletableFuture<U> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException("Null request member"));
        return future;
      }
      InternalMember member = cluster.member(uri);
      if (member == null) {
        CompletableFuture<U> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException("Invalid request URI"));
        return future;
      }
      return member.send(topic, 0, request);
    }
    @Override
    public void destroyRoutes(Cluster cluster, RaftProtocol protocol) {
      GlobalCluster globalCluster = (GlobalCluster) cluster;
      globalCluster.localMember().unregister(Topics.CONFIGURE, 0);
      globalCluster.localMember().unregister(Topics.PING, 0);
      globalCluster.localMember().unregister(Topics.POLL, 0);
      globalCluster.localMember().unregister(Topics.SYNC, 0);
      globalCluster.localMember().unregister(Topics.COMMIT, 0);
    }
  };

  public DefaultCopycatCoordinator(ClusterConfig config, Protocol protocol, Log log, ExecutionContext executor) {
    this.cluster = new GlobalCluster(config, protocol, router, executor);
    this.context = new DefaultCopycatStateContext(cluster, config, log, executor);
    cluster.setState(context);
    this.executor = executor;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Consumes an entry from the log.
   */
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    int entryType = entry.getInt();
    int nameLength = entry.getInt();
    byte[] nameBytes = new byte[nameLength];
    entry.get(nameBytes);
    String name = new String(nameBytes);
    int sourceLength = entry.getInt();
    byte[] sourceBytes = new byte[sourceLength];
    entry.get(sourceBytes);
    String source = new String(sourceBytes);
    switch (entryType) {
      case 1:
        return handleJoin(name, source);
      case 2:
        return handleLeave(name, source);
      case 3:
        return handleDelete(name, source);
      default:
        throw new IllegalStateException("Unknown entry type");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<CopycatContext> join(String name, LogFactory logFactory) {
    CompletableFuture<CopycatContext> future = new CompletableFuture<>();
    ByteBuffer entry = ByteBuffer.allocate(4 + 4 + name.getBytes().length + 4 + cluster.localMember().uri().getBytes().length);
    entry.putInt(1); // Entry type
    entry.putInt(name.getBytes().length);
    entry.put(name.getBytes());
    entry.putInt(cluster.localMember().uri().getBytes().length);
    entry.put(cluster.localMember().uri().getBytes());
    context.commit(entry).whenComplete((members, error) -> {
      if (error == null) {
        DefaultCopycatStateContext context = contexts.get(name);
        if (context == null) {
          ExecutionContext executor = ExecutionContext.create();
          CoordinatedCluster cluster = new CoordinatedCluster(name.hashCode(), this.cluster, new ResourceRouter(name), executor);
          context = new DefaultCopycatStateContext(cluster, new ClusterConfig().withLocalMember(this.cluster.localMember().uri()), logFactory.createLog(name), ExecutionContext.create());
          cluster.setState(context);
          contexts.put(name, context);
        }
        future.complete(context);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> leave(String name) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ByteBuffer entry = ByteBuffer.allocate(4 + 4 + name.getBytes().length + 4 + cluster.localMember().uri().getBytes().length);
    entry.putInt(2); // Entry type
    entry.putInt(name.getBytes().length);
    entry.put(name.getBytes());
    entry.putInt(cluster.localMember().uri().getBytes().length);
    entry.put(cluster.localMember().uri().getBytes());
    context.commit(entry).whenComplete((result, error) -> {
      if (error == null) {
        int removed = result.getInt();
        if (removed == 1) {
          DefaultCopycatStateContext context = contexts.remove(name);
          if (context != null) {
            context.close().whenComplete((r, e) -> {
              if (e == null) {
                future.complete(null);
              } else {
                future.completeExceptionally(e);
              }
            });
          } else {
            future.complete(null);
          }
        } else {
          future.complete(null);
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> delete(String name) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ByteBuffer entry = ByteBuffer.allocate(4 + 4 + name.getBytes().length + 4 + cluster.localMember()
      .uri()
      .getBytes().length);
    entry.putInt(3); // Entry type
    entry.putInt(name.getBytes().length);
    entry.put(name.getBytes());
    entry.putInt(cluster.localMember().uri().getBytes().length);
    entry.put(cluster.localMember().uri().getBytes());
    context.commit(entry).whenComplete((result, error) -> {
      if (error == null) {
        int removed = result.getInt();
        if (removed == 1) {
          DefaultCopycatStateContext context = contexts.remove(name);
          if (context != null) {
            context.close().whenComplete((r, e) -> {
              if (e == null) {
                future.complete(null);
              } else {
                future.completeExceptionally(e);
              }
            });
          } else {
            future.complete(null);
          }
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
   * Handles a join action.
   */
  private ByteBuffer handleJoin(String resource, String owner) {
    ResourceInfo holder = resources.get(resource);
    if (holder != null) {
      AtomicInteger count = holder.members.get(owner);
      if (count == null) {
        count = new AtomicInteger();
        holder.members.put(owner, count);
      }
      count.incrementAndGet();
    } else {
      holder = new ResourceInfo(resource);
      holder.members.put(owner, new AtomicInteger(1));
      resources.put(resource, holder);
    }

    DefaultCopycatStateContext context = contexts.get(resource);
    if (context != null && context.cluster().member(owner) == null) {
      Cluster cluster = context.cluster();
      Set<String> members = new HashSet<>(holder.members.keySet());
      members.remove(cluster.localMember().uri());
      cluster.configure(new ClusterConfig()
        .withLocalMember(cluster.localMember().uri())
        .withRemoteMembers(members));
    }
    return ByteBuffer.wrap(new byte[0]);
  }

  /**
   * Handles a leave action.
   */
  private ByteBuffer handleLeave(String resource, String owner) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4);
    ResourceInfo holder = resources.get(resource);
    if (holder != null) {
      AtomicInteger count = holder.members.get(owner);
      if (count != null && count.decrementAndGet() == 0) {
        holder.members.remove(owner);
        buffer.putInt(1);
        return buffer;
      }
    }
    buffer.putInt(0);
    return buffer;
  }

  /**
   * Handles a delete action.
   */
  private ByteBuffer handleDelete(String resource, String owner) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4);
    resources.remove(resource);
    DefaultCopycatStateContext context = contexts.remove(resource);
    if (context != null) {
      try {
        context.close().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      buffer.putInt(1);
      return buffer;
    }
    buffer.putInt(0);
    return buffer;
  }

  /**
   * Handles a cluster configuration change.
   */
  private CompletableFuture<ClusterConfig> configure(ClusterConfig config) {
    CompletableFuture<ClusterConfig> future = new CompletableFuture<>();
    int length = 4;
    for (String uri : config.getRemoteMembers()) {
      length += 4 + uri.getBytes().length;
    }
    ByteBuffer buffer = ByteBuffer.allocateDirect(length);
    buffer.putInt(0); // Entry type
    for (String uri : config.getRemoteMembers()) {
      buffer.putInt(uri.getBytes().length);
      buffer.put(uri.getBytes());
    }
    context.commit(buffer).whenComplete((result, error) -> {
      if (error == null) {
        int succeeded = result.getInt();
        if (succeeded == 1) {
          context.setMembers(config.getMembers());
        }
        future.complete(config);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> open() {
    return CompletableFuture.allOf(cluster.open(), context.open()).thenRun(() -> {
      context.consumer(this::consume);
      cluster.configureHandler(this::configure);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.anyOf(cluster.close(), context.close()).thenRun(() -> {
      context.consumer(null);
      cluster.configureHandler(null);
    });
  }

  /**
   * Resource info holder.
   */
  private static class ResourceInfo {
    private final String resource;
    private final Map<String, AtomicInteger> members = new HashMap<>();
    private ResourceInfo(String resource) {
      this.resource = resource;
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
      cluster.localMember().handler(Topics.PING, protocol::ping);
      cluster.localMember().handler(Topics.POLL, protocol::poll);
      cluster.localMember().handler(Topics.APPEND, protocol::append);
      cluster.localMember().handler(Topics.SYNC, protocol::sync);
      cluster.localMember().handler(Topics.COMMIT, protocol::commit);
      protocol.pingHandler(request -> handleOutboundRequest(Topics.PING, request, cluster));
      protocol.pollHandler(request -> handleOutboundRequest(Topics.POLL, request, cluster));
      protocol.appendHandler(request -> handleOutboundRequest(Topics.APPEND, request, cluster));
      protocol.syncHandler(request -> handleOutboundRequest(Topics.SYNC, request, cluster));
      protocol.commitHandler(request -> handleOutboundRequest(Topics.COMMIT, request, cluster));
    }

    /**
     * Handles an outbound protocol request.
     */
    private <T extends Request, U extends Response> CompletableFuture<U> handleOutboundRequest(String topic, T request, Cluster cluster) {
      Member member = cluster.member(request.member());
      if (member != null) {
        return member.send(topic, request);
      }
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException(String.format("Invalid URI %s", request.member())));
      return future;
    }

    @Override
    public void destroyRoutes(Cluster cluster, RaftProtocol protocol) {
      cluster.localMember().<Request, Response>handler(name, null);
      protocol.pingHandler(null);
      protocol.pollHandler(null);
      protocol.appendHandler(null);
      protocol.syncHandler(null);
      protocol.commitHandler(null);
    }
  }

}
