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

import io.atomix.Consistency;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.session.Session;
import io.atomix.manager.DeleteResource;

import java.util.concurrent.CompletableFuture;

/**
 * Resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceContext {
  private final long resource;
  private final CopycatClient client;
  private final Transport transport;
  private final ResourceSession session;

  /**
   * @throws NullPointerException if {@code client} is null
   */
  public ResourceContext(long resource, CopycatClient client, Transport transport) {
    this.resource = resource;
    this.client = Assert.notNull(client, "client");
    this.transport = Assert.notNull(transport, "transport");
    this.session = new ResourceSession(resource, client.session(), client.context());
  }

  /**
   * Returns the resource execution context.
   *
   * @return The resource execution context.
   */
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Returns the client transport.
   *
   * @return The client transport.
   */
  public Transport transport() {
    return transport;
  }

  /**
   * Returns the resource session.
   *
   * @return The resource session.
   */
  public Session session() {
    return session;
  }

  /**
   * Submits a resource command.
   *
   * @param command The command to submit.
   * @param consistency The command consistency.
   * @param <T> The command output type.
   * @return A completable future to be completed with the command result.
   * @throws NullPointerException if {@code command} is null
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Command<T> command, Consistency consistency) {
    return client.submit(ResourceCommand.builder()
      .withResource(resource)
      .withCommand(command)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Submits a resource query.
   *
   * @param query The query to submit.
   * @param consistency The query consistency.
   * @param <T> The query output type.
   * @return A completable future to be completed with the query result.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Query<T> query, Consistency consistency) {
    return client.submit(ResourceQuery.builder()
      .withResource(resource)
      .withQuery(query)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Deletes the resource.
   *
   * @return A completable future to be called once the resource has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return client.submit(DeleteResource.builder()
      .withResource(resource)
      .build())
      .thenApply(deleted -> null);
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d]", getClass().getSimpleName(), resource);
  }

}
