/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.impl;

import io.atomix.logging.Logger;
import io.atomix.logging.LoggerFactory;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.RaftStateMachine;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftCluster;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.error.ConfigurationException;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.utils.concurrent.Futures;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides a standalone implementation of the <a href="http://raft.github.io/">Raft consensus algorithm</a>.
 *
 * @see RaftStateMachine
 * @see RaftStorage
 */
public class DefaultRaftServer implements RaftServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftServer.class);

  protected final String name;
  protected final RaftServerProtocol protocol;
  protected final RaftServerContext context;
  private volatile CompletableFuture<RaftServer> openFuture;
  private volatile CompletableFuture<Void> closeFuture;
  private Consumer<RaftMember> electionListener;
  private volatile boolean started;

  public DefaultRaftServer(String name, RaftServerProtocol protocol, RaftServerContext context) {
    this.name = checkNotNull(name, "name cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.context = checkNotNull(context, "context cannot be null");
  }

  @Override
  public String serverName() {
    return name;
  }

  @Override
  public RaftCluster cluster() {
    return context.getCluster();
  }

  @Override
  public Role getRole() {
    return context.getRole();
  }

  @Override
  public void addRoleChangeListener(Consumer<Role> listener) {
    context.addStateChangeListener(listener);
  }

  @Override
  public void removeRoleChangeListener(Consumer<Role> listener) {
    context.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<RaftServer> bootstrap() {
    return bootstrap(Collections.EMPTY_LIST);
  }

  @Override
  public CompletableFuture<RaftServer> bootstrap(MemberId... cluster) {
    return bootstrap(Arrays.asList(cluster));
  }

  @Override
  public CompletableFuture<RaftServer> bootstrap(Collection<MemberId> cluster) {
    return start(() -> cluster().bootstrap(cluster));
  }

  @Override
  public CompletableFuture<RaftServer> join(MemberId... cluster) {
    return join(Arrays.asList(cluster));
  }

  @Override
  public CompletableFuture<RaftServer> join(Collection<MemberId> cluster) {
    return start(() -> cluster().join(cluster));
  }

  /**
   * Starts the server.
   */
  private CompletableFuture<RaftServer> start(Supplier<CompletableFuture<Void>> joiner) {
    if (started)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          CompletableFuture<RaftServer> future = new CompletableFuture<>();
          openFuture = future;
          joiner.get().whenComplete((result, error) -> {
            if (error == null) {
              if (cluster().getLeader() != null) {
                started = true;
                future.complete(this);
              } else {
                electionListener = leader -> {
                  if (electionListener != null) {
                    started = true;
                    future.complete(this);
                    cluster().removeLeaderElectionListener(electionListener);
                    electionListener = null;
                  }
                };
                cluster().addLeaderElectionListener(electionListener);
              }
            } else {
              future.completeExceptionally(error);
            }
          });
          return future.whenComplete((r, e) -> openFuture = null);
        }
      }
    }

    return openFuture.whenComplete((result, error) -> {
      if (error == null) {
        LOGGER.info("Server started successfully!");
      } else {
        LOGGER.warn("Failed to start server!");
      }
    });
  }

  /**
   * Returns a boolean indicating whether the server is running.
   *
   * @return Indicates whether the server is running.
   */
  public boolean isRunning() {
    return started;
  }

  /**
   * Shuts down the server without leaving the Raft cluster.
   *
   * @return A completable future to be completed once the server has been shutdown.
   */
  public CompletableFuture<Void> shutdown() {
    if (!started) {
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.getThreadContext().execute(() -> {
      started = false;
      context.transition(Role.INACTIVE);
      future.complete(null);
    });

    return future.whenCompleteAsync((result, error) -> {
      context.close();
      started = false;
    });
  }

  /**
   * Leaves the Raft cluster.
   *
   * @return A completable future to be completed once the server has left the cluster.
   */
  public CompletableFuture<Void> leave() {
    if (!started) {
      return CompletableFuture.completedFuture(null);
    }

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
          if (openFuture == null) {
            cluster().leave().whenComplete((leaveResult, leaveError) -> {
              shutdown().whenComplete((shutdownResult, shutdownError) -> {
                context.delete();
                closeFuture.complete(null);
              });
            });
          } else {
            openFuture.whenComplete((openResult, openError) -> {
              if (openError == null) {
                cluster().leave().whenComplete((leaveResult, leaveError) -> {
                  shutdown().whenComplete((shutdownResult, shutdownError) -> {
                    context.delete();
                    closeFuture.complete(null);
                  });
                });
              } else {
                closeFuture.complete(null);
              }
            });
          }
        }
      }
    }

    return closeFuture;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .toString();
  }

  /**
   * Default Raft server builder.
   */
  public static class Builder extends RaftServer.Builder {
    public Builder(MemberId localMemberId) {
      super(localMemberId);
    }

    @Override
    public RaftServer build() {
      if (stateMachineRegistry.size() == 0) {
        throw new ConfigurationException("No state machines registered");
      }

      // If the server name is null, set it to the member ID.
      if (name == null) {
        name = localMemberId.id();
      }

      // If the storage is not configured, create a new Storage instance with the configured serializer.
      if (storage == null) {
        storage = RaftStorage.newBuilder().build();
      }

      RaftServerContext context = new RaftServerContext(name, type, localMemberId, protocol, storage, stateMachineRegistry, threadPoolSize);
      context.setElectionTimeout(electionTimeout)
          .setHeartbeatInterval(heartbeatInterval)
          .setSessionTimeout(sessionTimeout);

      return new DefaultRaftServer(name, protocol, context);
    }
  }
}
