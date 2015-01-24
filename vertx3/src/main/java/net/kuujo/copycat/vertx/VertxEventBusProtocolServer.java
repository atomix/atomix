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

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyFailure;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;

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
  private MessageConsumer<byte[]> consumer;
  private ProtocolHandler handler;

  public VertxEventBusProtocolServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
    this.context = vertx.getOrCreateContext();
  }

  @Override
  public void handle(Message<byte[]> message) {
    if (handler != null) {
      handler.apply(ByteBuffer.wrap(message.body())).whenComplete((reply, error) -> {
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
    consumer = vertx.eventBus().<byte[]>consumer(address).handler(this);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    if (consumer != null) {
      consumer.unregister();
    }
    return CompletableFuture.completedFuture(null);
  }

}
