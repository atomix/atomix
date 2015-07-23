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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.protocol.PublishRequest;
import net.kuujo.copycat.transport.Connection;
import net.kuujo.copycat.util.concurrent.ComposableFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Local server session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LocalServerSession extends ServerSession {
  private final RaftServerState context;

  LocalServerSession(long id, Member member, UUID connection, RaftServerState context) {
    super(id, member, connection);
    this.context = context;
  }

  @Override
  ServerSession setConnection(Connection connection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    ComposableFuture<Void> future = new ComposableFuture<>();
    context.getContext().execute(() -> {
      context.getInternalState().publish(PublishRequest.builder()
        .withSession(id())
        .withMessage(message)
        .build())
        .<Void>thenApply(v -> null)
        .whenComplete(future);
    });
    return future;
  }

}
