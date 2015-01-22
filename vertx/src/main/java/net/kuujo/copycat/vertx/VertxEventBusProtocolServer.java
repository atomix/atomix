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

import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.ReplyFailure;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolServer implements ProtocolServer, Handler<Message<byte[]>> {
  private final String address;
  private final Vertx vertx;
  private final Context context;
  private ProtocolHandler handler;

  public VertxEventBusProtocolServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
    this.context = vertx.currentContext();
  }

  @Override
  public void handle(Message<byte[]> message) {
    if (handler != null) {
      handler.handle(ByteBuffer.wrap(message.body())).whenComplete((reply, error) -> {
        context.runOnContext(v -> {
          if (error != null) {
            message.fail(0, error.getMessage());
          } else {
            byte[] bytes = new byte[reply.remaining()];
            reply.get(bytes);
            message.reply(bytes);
          }
        });
      });
    } else {
      message.fail(ReplyFailure.NO_HANDLERS.toInt(), "No message handler registered");
    }
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().registerHandler(address, this, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().unregisterHandler(address, this, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

}
