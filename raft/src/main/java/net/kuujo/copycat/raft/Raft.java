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
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.state.RaftStateContext;
import net.kuujo.copycat.util.Context;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft algorithm.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Raft implements ManagedProtocol {

  /**
   * Raft state types.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public enum State {

    /**
     * Start state.
     */
    START,

    /**
     * Passive state.
     */
    PASSIVE,

    /**
     * Follower state.
     */
    FOLLOWER,

    /**
     * Candidate state.
     */
    CANDIDATE,

    /**
     * Leader state.
     */
    LEADER

  }

  /**
   * Returns a new Raft builder.
   *
   * @return A new Raft builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftStateContext context;
  private CompletableFuture<Protocol> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  private Raft(RaftStateContext context) {
    this.context = context;
  }

  /**
   * Returns the Raft cluster.
   *
   * @return The Raft cluster.
   */
  public Cluster cluster() {
    return context.getCluster();
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
  public State state() {
    return context.getState();
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return context.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (!open)
      throw new IllegalStateException("protocol not open");
    return context.submit(query);
  }

  @Override
  public CompletableFuture<Protocol> open() {
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

  @Override
  public CompletableFuture<Void> delete() {
    return close().thenRun(context::delete);
  }

  /**
   * Raft builder.
   */
  public static class Builder implements Protocol.Builder<Raft> {
    private Log log;
    private RaftConfig config = new RaftConfig();
    private StateMachine stateMachine;
    private ManagedCluster cluster;

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(ManagedCluster cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(Log log) {
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
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     */
    public Builder withSessionTimeout(long sessionTimeout) {
      config.setSessionTimeout(sessionTimeout);
      return this;
    }

    /**
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     */
    public Builder withSessionTimeout(long sessionTimeout, TimeUnit unit) {
      config.setSessionTimeout(sessionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft keep alive interval, returning the Raft configuration for method chaining.
     *
     * @param keepAliveInterval The Raft keep alive interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the keep alive interval is not positive
     */
    public Builder withKeepAliveInterval(long keepAliveInterval) {
      config.setKeepAliveInterval(keepAliveInterval);
      return this;
    }

    /**
     * Sets the Raft keep alive interval, returning the Raft configuration for method chaining.
     *
     * @param keepAliveInterval The Raft keep alive interval.
     * @param unit The keep alive interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the keep alive interval is not positive
     */
    public Builder withKeepAliveInterval(long keepAliveInterval, TimeUnit unit) {
      config.setKeepAliveInterval(keepAliveInterval, unit);
      return this;
    }

    @Override
    public Raft build() {
      if (stateMachine == null)
        throw new ConfigurationException("state machine not configured");
      if (cluster == null)
        throw new ConfigurationException("cluster not configured");
      if (log == null)
        throw new NullPointerException("log cannot be null");

      RaftStateContext context = (RaftStateContext) new RaftStateContext(log, stateMachine, cluster, Context.createContext(cluster.member().id() != 0 ? String.format("copycat-%d", cluster.member().id()) : "copycat", cluster.alleycat().clone()))
        .setHeartbeatInterval(config.getHeartbeatInterval())
        .setElectionTimeout(config.getElectionTimeout())
        .setSessionTimeout(config.getSessionTimeout())
        .setKeepAliveInterval(config.getKeepAliveInterval());
      return new Raft(context);
    }
  }

}
