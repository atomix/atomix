/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.raft.state.RaftContext;
import net.kuujo.copycat.raft.state.RaftState;
import net.kuujo.copycat.raft.storage.RaftStorage;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft algorithm.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Raft implements Managed<Raft> {

  /**
   * Returns a new Raft builder.
   *
   * @return A new Raft builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftContext context;
  private CompletableFuture<Raft> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  private Raft(RaftStorage log, RaftConfig config, StateMachine stateMachine, Cluster cluster, String topic, ExecutionContext context) {
    this.context = new RaftContext(log, stateMachine, cluster, topic, context)
      .setHeartbeatInterval(config.getHeartbeatInterval())
      .setElectionTimeout(config.getElectionTimeout());
  }

  /**
   * Returns the current Raft term.
   *
   * @return The current Raft term.
   */
  public long term() {
    return context.getTerm();
  }

  /**
   * Returns the current Raft leader.
   *
   * @return The current Raft leader.
   */
  public Member leader() {
    int leader = context.getLeader();
    return leader != 0 ? context.getCluster().member(leader) : null;
  }

  /**
   * Returns the Raft state.
   *
   * @return The Raft state.
   */
  public RaftState state() {
    return context.getState();
  }

  /**
   * Submits an operation.
   *
   * @param operation The operation to submit.
   * @param <R> The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  public <R> CompletableFuture<R> submit(Operation<R> operation) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return context.submit(operation);
  }

  @Override
  public CompletableFuture<Raft> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          if (closeFuture == null) {
            openFuture = context.open().thenApply(c -> {
              openFuture = null;
              open = true;
              return this;
            });
          } else {
            openFuture = closeFuture.thenCompose(v -> context.open().thenApply(c -> {
              openFuture = null;
              open = true;
              return this;
            }));
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            });
          } else {
            closeFuture = openFuture.thenCompose(c -> context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            }));
          }
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Raft builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Raft> {
    private RaftStorage log;
    private RaftConfig config = new RaftConfig();
    private StateMachine stateMachine;
    private Cluster cluster;
    private String topic;
    private ExecutionContext context;

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(RaftStorage log) {
      this.log = log;
      return this;
    }

    /**
     * Sets the Raft state machine.
     *
     * @param stateMachine The Raft state machine.
     * @return The Raft builder.
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout) {
      config.setElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout, TimeUnit unit) {
      config.setElectionTimeout(electionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval) {
      config.setHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval.
     * @param unit The heartbeat interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
      config.setHeartbeatInterval(heartbeatInterval, unit);
      return this;
    }

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the Raft cluster topic.
     *
     * @param topic The Raft cluster topic.
     * @return The Raft builder.
     */
    public Builder withTopic(String topic) {
      this.topic = topic;
      return this;
    }

    /**
     * Sets the Raft execution context.
     *
     * @param context The execution context.
     * @return The Raft builder.
     */
    public Builder withContext(ExecutionContext context) {
      this.context = context;
      return this;
    }

    @Override
    public Raft build() {
      if (log == null)
        throw new ConfigurationException("log not configured");
      if (stateMachine == null)
        throw new ConfigurationException("state machine not configured");
      if (cluster == null)
        throw new ConfigurationException("cluster not configured");
      if (topic == null)
        throw new ConfigurationException("topic not configured");
      return new Raft(log, config, stateMachine, cluster, topic, context != null ? context : new ExecutionContext("copycat-" + topic));
    }
  }

}
