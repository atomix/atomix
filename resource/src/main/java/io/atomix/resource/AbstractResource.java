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
package io.atomix.resource;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for fault-tolerant stateful distributed objects.
 * <p>
 * Resources are stateful distributed objects that run across a set of {@link io.atomix.copycat.server.CopycatServer}s
 * in a cluster.
 * <p>
 * Resources can be created either as standalone {@link io.atomix.copycat.server.StateMachine}s in
 * a typical Copycat cluster or as {@code Atomix} resources. Operations on the resource are translated into
 * {@link Command}s and {@link Query}s which are submitted to the cluster where they're logged and
 * replicated.
 * <p>
 * Resources have varying consistency guarantees depending on the configured resource {@link Consistency}
 * and the semantics of the specific resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResource implements Resource {
  protected final CopycatClient client;
  private Consistency consistency = Consistency.ATOMIC;

  protected AbstractResource(CopycatClient client) {
    this.client = client;
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public Resource with(Consistency consistency) {
    this.consistency = Assert.notNull(consistency, "consistency");
    return this;
  }

  /**
   * Submits a write operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the configured {@link Consistency#writeConsistency()} if
   * it does not explicitly override {@link Command#consistency()} to provide a static consistency level.
   *
   * @param command The command to submit.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   * @throws NullPointerException if {@code command} is null
   */
  protected <R> CompletableFuture<R> submit(Command<R> command) {
    return client.submit(new ResourceCommand<>(Assert.notNull(command, "command"), consistency.writeConsistency()));
  }

  /**
   * Submits a read operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the configured {@link Consistency#readConsistency()} if
   * it does not explicitly override {@link Query#consistency()} to provide a static consistency level.
   *
   * @param query The query to submit.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   * @throws NullPointerException if {@code query} is null
   */
  protected <R> CompletableFuture<R> submit(Query<R> query) {
    return client.submit(new ResourceQuery<>(Assert.notNull(query, "query"), consistency.readConsistency()));
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.submit(new ResourceStateMachine.DeleteCommand());
  }

  @Override
  public int hashCode() {
    return 37 * 23 + (int)(client.session().id() ^ (client.session().id() >>> 32));
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isAssignableFrom(object.getClass()) && ((AbstractResource) object).client.session().id() == client.session().id();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), client.session().id());
  }

}
