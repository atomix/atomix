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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.ManagedCluster;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.raft.CommitHandler;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.raft.RaftConfig;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.protocol.CommandRequest;
import net.kuujo.copycat.raft.protocol.QueryRequest;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceContext implements Managed<ResourceContext> {
  private final String name;
  private final ResourceConfig<?> config;
  private final RaftContext context;
  private final ManagedCluster cluster;
  private final Serializer serializer;
  private final ScheduledExecutorService scheduler;
  private final Executor executor;
  private volatile boolean open;

  public ResourceContext(String name, ResourceConfig<?> config, ClusterConfig cluster) {
    this(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d")), Executors.newSingleThreadExecutor(new NamedThreadFactory(name)));
  }

  public ResourceContext(String name, ResourceConfig<?> config, ClusterConfig cluster, Executor executor) {
    this(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d")), executor);
  }

  public ResourceContext(String name, ResourceConfig<?> config, ClusterConfig cluster, ScheduledExecutorService scheduler, Executor executor) {
    this.name = Assert.isNotNull(name, "name");
    this.config = Assert.isNotNull(config, "config").resolve();
    RaftConfig raftConfig = new RaftConfig(this.config.toMap());
    if (raftConfig.getReplicas().isEmpty()) {
      raftConfig.setReplicas(cluster.getMembers());
    }
    this.serializer = this.config.getSerializer();
    this.scheduler = Assert.isNotNull(scheduler, "scheduler");
    this.executor = Assert.isNotNull(executor, "executor");
    this.context = new RaftContext(name, cluster.getLocalMember(), raftConfig, scheduler);
    this.cluster = new ManagedCluster(cluster.getProtocol(), this);
  }

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  @SuppressWarnings("unchecked")
  public <T extends ResourceConfig<?>> T config() {
    return (T) config;
  }

  /**
   * Returns the resource status.
   *
   * @return The resource status.
   */
  public ResourceState state() {
    return context.isRecovering() ? ResourceState.RECOVER : ResourceState.HEALTHY;
  }

  /**
   * Returns the Raft context.
   *
   * @return The Raft context.
   */
  public RaftContext raft() {
    return context;
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the Copycat log.
   *
   * @return The Copycat log.
   */
  public LogManager log() {
    return context.log();
  }

  /**
   * Returns the internal scheduler.
   *
   * @return The internal scheduler.
   */
  public ScheduledExecutorService scheduler() {
    return scheduler;
  }

  /**
   * Returns the context serializer.
   *
   * @return The context serializer.
   */
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Returns the context executor.
   *
   * @return The context executor.
   */
  public Executor executor() {
    return executor;
  }

  /**
   * Registers an entry commit handler on the context.
   *
   * @param handler The entry commit handler.
   * @return The Copycat context.
   */
  public synchronized ResourceContext commitHandler(CommitHandler handler) {
    context.commitHandler(handler);
    return this;
  }

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  public synchronized CompletableFuture<ByteBuffer> query(ByteBuffer entry) {
    return query(entry, Consistency.DEFAULT);
  }

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  public synchronized CompletableFuture<ByteBuffer> query(ByteBuffer entry, Consistency consistency) {
    if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    QueryRequest request = QueryRequest.builder()
      .withUri(context.getLocalMember().uri())
      .withEntry(entry)
      .withConsistency(consistency)
      .build();
    context.query(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.result());
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Submits a persistent entry to the context.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  public synchronized CompletableFuture<ByteBuffer> commit(ByteBuffer entry) {
    if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    CommandRequest request = CommandRequest.builder()
      .withUri(context.getLocalMember().uri())
      .withEntry(entry)
      .build();
    context.command(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.result());
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
  public synchronized CompletableFuture<ResourceContext> open() {
    return cluster.open().thenComposeAsync(v -> context.open(), context.executor()).thenRun(() -> open = true).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    open = false;
    return context.close().thenCompose(v -> cluster.close());
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
