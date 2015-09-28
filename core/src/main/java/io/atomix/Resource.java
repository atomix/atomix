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
package io.atomix;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.StateMachine;
import io.atomix.resource.ResourceContext;

import java.util.concurrent.CompletableFuture;

/**
 * Atomix resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Resource<T extends Resource<T>> {
  protected ResourceContext context;

  /**
   * Initializes the resource.
   *
   * @param context The resource context.
   * @throws NullPointerException if {@code context} is null
   */
  protected void open(ResourceContext context) {
    this.context = Assert.notNull(context, "context");
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
   * @throws NullPointerException if {@code command} is null
   */
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return context.submit(Assert.notNull(command, "command"));
  }

  /**
   * Submits a query to the Raft protocol.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   * @throws NullPointerException if {@code query} is null
   */
  protected <T> CompletableFuture<T> submit(Query<T> query) {
    return context.submit(Assert.notNull(query, "query"));
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
