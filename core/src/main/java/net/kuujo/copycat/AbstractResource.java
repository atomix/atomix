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
package net.kuujo.copycat;

import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.Raft;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource implements Resource {
  protected final Raft protocol;

  protected AbstractResource(Raft protocol) {
    this.protocol = protocol;
  }

  /**
   * Submits an operation to the Raft protocol.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  protected <T> CompletableFuture<T> submit(Operation<T> operation) {
    return protocol.submit(operation);
  }

  /**
   * Submits a command to the Raft protocol.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return protocol.submit(command);
  }

  /**
   * Submits a query to the Raft protocol.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  protected <T> CompletableFuture<T> submit(Query<T> query) {
    return protocol.submit(query);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return protocol.delete();
  }

}
