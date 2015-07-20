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
package net.kuujo.copycat.raft.client.state;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.protocol.PublishRequest;
import net.kuujo.copycat.raft.protocol.PublishResponse;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.transport.Connection;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Client session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClientSession extends Session {
  private final Listeners<Object> listeners = new Listeners<>();
  private final Context context;

  public ClientSession(long id, int member, UUID connection, Context context) {
    super(id, member, connection);
    this.context = context;
  }

  /**
   * Sets the session connection.
   */
  protected void setConnection(Connection connection) {
    connection.handler(PublishRequest.class, this::handlePublish);
  }

  @Override
  protected void expire() {
    super.expire();
  }

  @Override
  protected void close() {
    super.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenerContext<?> onReceive(Listener listener) {
    return listeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    context.execute(() -> {
      for (Listener<Object> listener : listeners) {
        listener.accept(message);
      }
    });
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    for (Listener listener : listeners) {
      listener.accept(request.message());
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .build());
  }

}
