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
package net.kuujo.copycat.vertx;

import io.vertx.core.*;
import io.vertx.core.eventbus.*;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolHandler;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x event bus protocol connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocolConnection implements ProtocolConnection, Handler<Message<byte[]>> {
  private final String id;
  private String address;
  private final Vertx vertx;
  private final Context context;
  private final DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(5000);
  private final MessageConsumer<byte[]> consumer;
  private ProtocolHandler handler;
  private EventListener<Void> closeListener;

  public VertxEventBusProtocolConnection(String id, Vertx vertx, Handler<AsyncResult<VertxEventBusProtocolConnection>> doneHandler) {
    this(id, null, vertx, doneHandler);
  }

  public VertxEventBusProtocolConnection(String id, String address, Vertx vertx, Handler<AsyncResult<VertxEventBusProtocolConnection>> doneHandler) {
    this.id = id;
    this.address = address;
    this.vertx = vertx;
    this.context = vertx.getOrCreateContext();
    this.consumer = vertx.eventBus().consumer(id, this);
    this.consumer.completionHandler(result -> {
      if (result.failed()) {
        Future.<VertxEventBusProtocolConnection>failedFuture(result.cause()).setHandler(doneHandler);
      } else {
        Future.succeededFuture(this).setHandler(doneHandler);
      }
    });
  }

  /**
   * Sets the connection address.
   */
  public VertxEventBusProtocolConnection address(String address) {
    this.address = address;
    return this;
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
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
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    byte[] bytes = new byte[request.remaining()];
    request.get(bytes);
    if (context != null) {
      context.runOnContext(v -> {
        vertx.eventBus().send(address, bytes, deliveryOptions, (Handler<AsyncResult<Message<byte[]>>>) result -> {
          if (result.succeeded()) {
            future.complete(ByteBuffer.wrap(result.result().body()));
          } else {
            ReplyException exception = (ReplyException) result.cause();
            if (exception.failureType() == ReplyFailure.NO_HANDLERS || exception.failureType() == ReplyFailure.TIMEOUT) {
              future.completeExceptionally(new ProtocolException(exception));
            } else {
              future.completeExceptionally(new CopycatException(exception.getMessage()));
            }
          }
        });
      });
    } else {
      vertx.eventBus().send(address, bytes, deliveryOptions, (Handler<AsyncResult<Message<byte[]>>>) result -> {
        if (result.succeeded()) {
          future.complete(ByteBuffer.wrap(result.result().body()));
        } else {
          ReplyException exception = (ReplyException) result.cause();
          if (exception.failureType() == ReplyFailure.NO_HANDLERS || exception.failureType() == ReplyFailure.TIMEOUT) {
            future.completeExceptionally(new ProtocolException(exception));
          } else {
            future.completeExceptionally(new CopycatException(exception.getMessage()));
          }
        }
      });
    }
    return future;
  }

  @Override
  public ProtocolConnection closeListener(EventListener<Void> listener) {
    this.closeListener = listener;
    return this;
  }

  @Override
  public ProtocolConnection exceptionListener(EventListener<Throwable> listener) {
    return this;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    consumer.unregister(result -> {
      if (closeListener != null) {
        closeListener.accept(null);
      }
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(null);
      }
    });
    return future;
  }

}
