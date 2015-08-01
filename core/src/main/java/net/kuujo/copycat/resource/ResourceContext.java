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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.manager.DeleteResource;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.client.RaftClient;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.concurrent.CompletableFuture;

/**
 * Resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceContext {
  private final long resource;
  private final RaftClient client;
  private final ResourceSession session;

  public ResourceContext(long resource, RaftClient client) {
    this.resource = resource;
    this.client = client;
    this.session = new ResourceSession(resource, client.session(), client.context());
  }

  /**
   * Returns the resource execution context.
   *
   * @return The resource execution context.
   */
  public Context context() {
    return client.context();
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
   * @param <T> The command output type.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Command<T> command) {
    return client.submit(ResourceCommand.builder()
      .withResource(resource)
      .withCommand(command)
      .build());
  }

  /**
   * Submits a resource query.
   *
   * @param query The query to submit.
   * @param <T> The query output type.
   * @return A completable future to be completed with the query result.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return client.submit(ResourceQuery.builder()
      .withResource(resource)
      .withQuery(query)
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

}
