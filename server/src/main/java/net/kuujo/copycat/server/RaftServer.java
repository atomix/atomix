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
package net.kuujo.copycat.server;

import net.kuujo.copycat.*;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderResolver;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.server.state.ServerContext;
import net.kuujo.copycat.transport.Transport;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftServer implements Raft, Managed<Raft> {

  /**
   * Raft state types.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public enum State {

    /**
     * Start state.
     */
    INACTIVE,

    /**
     * Join state.
     */
    JOIN,

    /**
     * Leave state.
     */
    LEAVE,

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

  private final ServerContext context;
  private CompletableFuture<Raft> openFuture;
  private CompletableFuture<Void> closeFuture;
  private boolean open;

  private RaftServer(ServerContext context) {
    this.context = context;
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
    return context.getLeader();
  }

  @Override
  public Context context() {
    return context.getContext();
  }

  @Override
  public Session session() {
    return context.getSession();
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

  @Override
  public CompletableFuture<Void> delete() {
    return close().thenRun(context::delete);
  }

  /**
   * Raft server builder.
   */
  public static class Builder extends Raft.Builder<Builder, RaftServer> {
    private Transport transport;
    private Log log;
    private Serializer serializer;
    private RaftConfig config = new RaftConfig();
    private StateMachine stateMachine;
    private int memberId;
    private Members members;

    private Builder() {
    }

    @Override
    protected void reset() {
      transport = null;
      log = null;
      serializer = null;
      config = new RaftConfig();
      stateMachine = null;
      memberId = 0;
      members = null;
    }

    @Override
    public Builder withTransport(Transport transport) {
      this.transport = transport;
      return this;
    }

    /**
     * Sets the server member ID.
     *
     * @param memberId The server member ID.
     * @return The Raft builder.
     */
    public Builder withMemberId(int memberId) {
      this.memberId = memberId;
      return this;
    }

    /**
     * Sets the voting Raft members.
     *
     * @param members The voting Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Members members) {
      this.members = members;
      return this;
    }

    /**
     * Sets the Raft serializer.
     *
     * @param serializer The Raft serializer.
     * @return The Raft builder.
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = serializer;
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
    public RaftServer build() {
      if (stateMachine == null)
        throw new ConfigurationException("state machine not configured");
      if (transport == null)
        throw new ConfigurationException("protocol not configured");
      if (members == null)
        throw new ConfigurationException("members not configured");
      if (log == null)
        throw new ConfigurationException("log not configured");

      // If no serializer instance was provided, create one.
      if (serializer == null) {
        serializer = new Serializer();
      }

      // Resolve serializer serializable types with the ServiceLoaderResolver.
      serializer.resolve(new ServiceLoaderResolver());

      ServerContext context = (ServerContext) new ServerContext(memberId, members, transport, log, stateMachine, serializer)
        .setHeartbeatInterval(config.getHeartbeatInterval())
        .setElectionTimeout(config.getElectionTimeout())
        .setSessionTimeout(config.getSessionTimeout())
        .setKeepAliveInterval(config.getKeepAliveInterval());
      return new RaftServer(context);
    }
  }

}
