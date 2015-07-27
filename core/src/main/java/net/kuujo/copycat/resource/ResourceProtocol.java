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
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.Raft;
import net.kuujo.copycat.Session;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.concurrent.CompletableFuture;

/**
 * Resource protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceProtocol implements Raft {
  private final long resource;
  private final Raft protocol;
  private final ResourceSession session;

  public ResourceProtocol(long resource, Raft protocol) {
    this.resource = resource;
    this.protocol = protocol;
    this.session = new ResourceSession(resource, protocol.session(), protocol.context());
  }

  @Override
  public Context context() {
    return protocol.context();
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Command<T> command) {
    return protocol.submit(ResourceCommand.builder()
      .withResource(resource)
      .withCommand(command)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return protocol.submit(ResourceQuery.builder()
      .withResource(resource)
      .withQuery(query)
      .build());
  }

  @Override
  public CompletableFuture<Void> delete() {
    return protocol.submit(DeleteResource.builder()
      .withResource(resource)
      .build())
      .thenApply(deleted -> null);
  }

}
