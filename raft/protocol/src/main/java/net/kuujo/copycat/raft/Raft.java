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
   * Deletes the protocol.
   *
   * @return The deleted protocol.
   */
  CompletableFuture<Void> delete();

  /**
   * Protocol builder.
   */
  static interface Builder<T extends Raft> extends net.kuujo.copycat.Builder<T> {
  }

}
