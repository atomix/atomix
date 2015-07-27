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

import net.kuujo.copycat.transport.Transport;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Primary interface for interacting with the Raft consensus algorithm.
 * <p>
 * Copycat does not make significant distinction between clients and servers. It is designed to provide a common interface
 * regardless of the type of node, and this interface is provided as a common interface for interacting with Copycat's
 * Raft implementation.
 * <p>
 * Once a {@link Raft} instance is started - whether it's a client or server - the Raft instance connects to other servers
 * in the cluster and opens a {@link net.kuujo.copycat.raft.Session}. The provided {@link #session()} will be opened once
 * the client or server has safely connected to the rest of the cluster.
 * <p>
 * The Raft implementation is designed simply to support two types of operations - {@link net.kuujo.copycat.raft.Command commands}
 * and {@link net.kuujo.copycat.raft.Query queries}. Commands {@link #submit(Command) submitted} via this interface will
 * always be forwarded to the cluster leader, and queries {@link #submit(Query) submitted} will be sent to the nearest server.
 * <p>
 * Implementations of this interface must be thread safe. Additionally, they must guarantee that asynchronous
 * {@link java.util.concurrent.CompletableFuture} callbacks always be executed on the same thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Raft {

  /**
   * Returns the Raft execution context.
   * <p>
   * The execution context is the event loop that this Raft instance uses to communicate with clients and servers.
   * Implementations must guarantee that all asynchronous {@link java.util.concurrent.CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link net.kuujo.copycat.util.concurrent.Context}.
   * <p>
   * The {@link net.kuujo.copycat.util.concurrent.Context} can also be used to access the Raft instance's internal
   * {@link net.kuujo.alleycat.Alleycat serializer} via {@link Context#serializer()}.
   *
   * @return The Raft context.
   */
  Context context();

  /**
   * Returns the instance session.
   * <p>
   * The returned {@link net.kuujo.copycat.raft.Session} instance will remain constant throughout the lifetime of this
   * Raft instance. Once the instance is opened, the session will be registered with the Raft cluster and listeners
   * registered via {@link net.kuujo.copycat.raft.Session#onOpen(net.kuujo.copycat.Listener)} will be called. In the
   * event of a session expiration, listeners registered via {@link net.kuujo.copycat.raft.Session#onClose(net.kuujo.copycat.Listener)}
   * will be called.
   *
   * @return The instance session.
   */
  Session session();

  /**
   * Submits an operation to the Raft protocol.
   * <p>
   * This method is provided for convenience. The submitted {@link net.kuujo.copycat.raft.Operation} must be an instance
   * of {@link net.kuujo.copycat.raft.Command} or {@link net.kuujo.copycat.raft.Query}.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   * @throws java.lang.IllegalArgumentException If the {@link net.kuujo.copycat.raft.Operation} is not an instance of
   * either {@link net.kuujo.copycat.raft.Command} or {@link net.kuujo.copycat.raft.Query}.
   */
  default <T> CompletableFuture<T> submit(Operation<T> operation) {
    if (operation instanceof Command) {
      return submit((Command<T>) operation);
    } else if (operation instanceof Query) {
      return submit((Query<T>) operation);
    } else {
      throw new IllegalArgumentException("unknown operation type");
    }
  }

  /**
   * Submits a command to the Raft protocol.
   * <p>
   * Commands are used to alter state machine state. All commands will be forwarded to the current Raft leader.
   * Once a leader receives the command, it will write the command to its internal {@link net.kuujo.copycat.log.Log} and
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
  <T> CompletableFuture<T> submit(Command<T> command);

  /**
   * Submits a query to the Raft protocol.
   * <p>
   * Queries are used to read state machine state. The behavior of query submissions is primarily dependent on the
   * query's {@link net.kuujo.copycat.raft.ConsistencyLevel}. For {@link net.kuujo.copycat.raft.ConsistencyLevel#LINEARIZABLE}
   * and {@link net.kuujo.copycat.raft.ConsistencyLevel#LINEARIZABLE_LEASE} consistency levels, queries will be forwarded
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
  <T> CompletableFuture<T> submit(Query<T> query);

  /**
   * Deletes the Raft instance.
   * <p>
   * If the instance is stateful, internal persistent state will be deleted.
   *
   * @return The deleted Raft instance.
   */
  CompletableFuture<Void> delete();

  /**
   * Raft instance builder.
   */
  static abstract class Builder<T extends Builder<T, U>, U extends Raft> extends net.kuujo.copycat.Builder<U> {

    /**
     * Sets the network transport.
     * <p>
     * The provided {@link net.kuujo.copycat.transport.Transport} will be used to connect to provided
     * {@link net.kuujo.copycat.raft.Members} to join the cluster at startup and participate in all aspects of the Raft
     * consensus protocol.
     *
     * @param transport The network protocol.
     * @return The Raft builder.
     */
    public abstract T withTransport(Transport transport);

    /**
     * Sets the Raft members.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    public T withMembers(Member... members) {
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
    public T withMembers(Collection<Member> members) {
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
    public abstract T withMembers(Members members);

  }

}
