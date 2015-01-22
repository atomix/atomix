/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.vertx;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.protocol.ProtocolClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolClient implements ProtocolClient {
  private final String address;
  private Vertx vertx;

  public VertxEventBusProtocolClient(String address, Vertx vertx) {
    this.address = Assert.isNotNull(address, "Vert.x event bus address cannot be null");
    this.vertx = Assert.isNotNull(vertx, "Vert.x instance cannot be null");
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    vertx.eventBus().sendWithTimeout(address, request.array(), 5000, (Handler<AsyncResult<Message<byte[]>>>) result -> {
      if (result.succeeded()) {
        future.complete(ByteBuffer.wrap(result.result().body()));
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> connect() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

}
