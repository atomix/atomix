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

import net.kuujo.copycat.raft.protocol.Protocol;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Raft {

  /**
   * Returns the Raft sessions.
   *
   * @return The Raft sessions.
   */
  Sessions sessions();

  /**
   * Returns the local session.
   *
   * @return The local session.
   */
  Session session();

  /**
   * Submits an operation to the Raft protocol.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
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
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  <T> CompletableFuture<T> submit(Command<T> command);

  /**
   * Submits a query to the Raft protocol.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  <T> CompletableFuture<T> submit(Query<T> query);

  /**
   * Deletes the Raft instance.
   *
   * @return The deleted Raft instance.
   */
  CompletableFuture<Void> delete();

  /**
   * Raft builder.
   */
  static interface Builder<T extends Builder<T, U>, U extends Raft> extends net.kuujo.copycat.Builder<U> {

    /**
     * Sets the network protocol.
     *
     * @param protocol The network protocol.
     * @return The Raft builder.
     */
    T withProtocol(Protocol protocol);

    /**
     * Sets the Raft members.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    default T withMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the Raft members.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    default T withMembers(Collection<Member> members) {
      return withMembers(Members.builder().withMembers(members).build());
    }

    /**
     * Sets the Raft members.
     *
     * @param members The Raft members.
     * @return The Raft builder.
     */
    T withMembers(Members members);

  }

}
