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
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.resource.ResourceContext;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Resource {
  protected ResourceContext context;

  /**
   * Initializes the resource.
   *
   * @param context The resource context.
   */
  protected void open(ResourceContext context) {
    this.context = context;
  }

  /**
   * Returns the resource state machine class.
   *
   * @return The resource state machine class.
   */
  protected abstract Class<? extends StateMachine> stateMachine();

  /**
   * Submits a command to the Raft protocol.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return context.submit(command);
  }

  /**
   * Submits a query to the Raft protocol.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  protected <T> CompletableFuture<T> submit(Query<T> query) {
    return context.submit(query);
  }

  /**
   * Deletes the resource.
   *
   * @return A completable future to be called once the resource has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return context.delete();
  }

}
