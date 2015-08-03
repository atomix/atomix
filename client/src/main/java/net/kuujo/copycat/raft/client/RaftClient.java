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
package net.kuujo.copycat.raft.client;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderResolver;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftClient implements Managed<RaftClient> {

  /**
   * Returns a new Raft client builder.
   *
   * @return A new Raft client builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Transport transport;
  private final Members members;
  private final Serializer serializer;
  private final long keepAliveInterval;
  private ClientSession session;
  private CompletableFuture<RaftClient> openFuture;
  private CompletableFuture<Void> closeFuture;

  protected RaftClient(Transport transport, Members members, Serializer serializer, long keepAliveInterval) {
    this.transport = transport;
    this.members = members;
    this.serializer = serializer;
    this.keepAliveInterval = keepAliveInterval;
  }

  /**
   * Returns the client execution context.
   * <p>
   * The execution context is the event loop that this client uses to communicate Raft servers.
   * Implementations must guarantee that all asynchronous {@link java.util.concurrent.CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link net.kuujo.copycat.util.concurrent.Context}.
   * <p>
   * The {@link net.kuujo.copycat.util.concurrent.Context} can also be used to access the Raft client's internal
   * {@link net.kuujo.copycat.io.serializer.Serializer serializer} via {@link Context#serializer()}.
   *
   * @return The Raft context.
   */
  public Context context() {
    return session != null ? session.context() : null;
  }

  /**
   * Returns the client session.
   * <p>
   * The returned {@link Session} instance will remain constant throughout the lifetime of this client. Once the instance
   * is opened, the session will have been registered with the Raft cluster and listeners registered via
   * {@link Session#onOpen(net.kuujo.copycat.Listener)} will be called. In the event of a session expiration, listeners
   * registered via {@link Session#onClose(net.kuujo.copycat.Listener)} will be called.
   *
   * @return The client session.
   */
  public Session session() {
    return session;
  }

  /**
   * Submits an operation to the Raft cluster.
   * <p>
   * This method is provided for convenience. The submitted {@link Operation} must be an instance
   * of {@link Command} or {@link Query}.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   * @throws java.lang.IllegalArgumentException If the {@link Operation} is not an instance of
   * either {@link Command} or {@link Query}.
   */
  public <T> CompletableFuture<T> submit(Operation<T> operation) {
    if (operation instanceof Command) {
      return submit((Command<T>) operation);
    } else if (operation instanceof Query) {
      return submit((Query<T>) operation);
    } else {
      throw new IllegalArgumentException("unknown operation type");
    }
  }

  /**
   * Submits a command to the Raft cluster.
   * <p>
   * Commands are used to alter state machine state. All commands will be forwarded to the current Raft leader.
   * Once a leader receives the command, it will write the command to its internal {@link net.kuujo.copycat.io.storage.Log} and
   * replicate it to a majority of the cluster. Once the command has been replicated to a majority of the cluster, it
   * will apply the command to its state machine and respond with the result.
   * <p>
   * Once the command has been applied to a server state machine, the returned {@link java.util.concurrent.CompletableFuture}
   * will be completed with the state machine output.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (session == null)
      return Futures.exceptionalFuture(new IllegalStateException("client not open"));
    return session.submit(command);
  }

  /**
   * Submits a query to the Raft cluster.
   * <p>
   * Queries are used to read state machine state. The behavior of query submissions is primarily dependent on the
   * query's {@link ConsistencyLevel}. For {@link ConsistencyLevel#LINEARIZABLE}
   * and {@link ConsistencyLevel#LINEARIZABLE_LEASE} consistency levels, queries will be forwarded
   * to the Raft leader. For lower consistency levels, queries are allowed to read from followers. All queries are executed
   * by applying queries to an internal server state machine.
   * <p>
   * Once the query has been applied to a server state machine, the returned {@link java.util.concurrent.CompletableFuture}
   * will be completed with the state machine output.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (session == null)
      return Futures.exceptionalFuture(new IllegalStateException("client not open"));
    return session.submit(query);
  }

  @Override
  public CompletableFuture<RaftClient> open() {
    if (session != null && session.isOpen())
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          ClientSession session = new ClientSession(transport, members, keepAliveInterval, serializer);
          if (closeFuture == null) {
            openFuture = session.open().thenApply(s -> {
              synchronized (this) {
                openFuture = null;
                this.session = session;
                return this;
              }
            });
          } else {
            openFuture = closeFuture.thenCompose(v -> session.open().thenApply(s -> {
              synchronized (this) {
                openFuture = null;
                this.session = session;
                return this;
              }
            }));
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return session != null && session.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    if (session == null || !session.isOpen())
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (session == null) {
          return CompletableFuture.completedFuture(null);
        }

        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = session.close().whenComplete((result, error) -> {
              synchronized (this) {
                session = null;
                closeFuture = null;
              }
            });
          } else {
            closeFuture = openFuture.thenCompose(v -> session.close().whenComplete((result, error) -> {
              synchronized (this) {
                session = null;
                closeFuture = null;
              }
            }));
          }
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return session == null || session.isClosed();
  }

  /**
   * Raft client builder.
   */
  public static class Builder extends net.kuujo.copycat.util.Builder<RaftClient> {
    private Transport transport;
    private Serializer serializer;
    private long keepAliveInterval = 1000;
    private Members members;

    private Builder() {
    }

    @Override
    protected void reset() {
      transport = null;
      serializer = null;
      keepAliveInterval = 1000;
      members = null;
    }

    /**
     * Sets the client transport.
     *
     * @param transport The client transport.
     * @return The client builder.
     */
    public Builder withTransport(Transport transport) {
      this.transport = transport;
      return this;
    }

    /**
     * Sets the client serializer.
     *
     * @param serializer The client serializer.
     * @return The client builder.
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = serializer;
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval) {
      if (keepAliveInterval <= 0)
        throw new IllegalArgumentException("keep alive interval must be positive");
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @param unit The keep alive interval time unit.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval, TimeUnit unit) {
      return withKeepAliveInterval(unit.toMillis(keepAliveInterval));
    }

    /**
     * Sets the Raft members.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the Raft members.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Collection<Member> members) {
      return withMembers(Members.builder().withMembers(members).build());
    }

    /**
     * Sets the Raft members.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    public Builder withMembers(Members members) {
      this.members = members;
      return this;
    }

    @Override
    public RaftClient build() {
      // If no serializer instance was provided, create one.
      if (serializer == null) {
        serializer = new Serializer();
      }

      // Resolve serializer serializable types with the ServiceLoaderResolver.
      serializer.resolve(new ServiceLoaderResolver());

      return new RaftClient(transport, members, serializer, keepAliveInterval);
    }
  }

}
