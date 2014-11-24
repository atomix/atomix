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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.spi.protocol.ProtocolServer;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;

import java.util.concurrent.CompletableFuture;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusServer implements ProtocolServer {
  private final ProtocolReader reader = new ProtocolReader();
  private final ProtocolWriter writer = new ProtocolWriter();
  private final String address;
  private Vertx vertx;
  private RequestHandler requestHandler;

  private final Handler<Message<byte[]>> messageHandler = new Handler<Message<byte[]>>() {
    @Override
    public void handle(Message<byte[]> message) {
      if (requestHandler != null) {
        Request request = reader.readRequest(message.body());
        if (request instanceof PingRequest) {
          requestHandler.ping((PingRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        } else if (request instanceof SyncRequest) {
          requestHandler.sync((SyncRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        } else if (request instanceof PollRequest) {
          requestHandler.poll((PollRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        } else if (request instanceof SubmitRequest) {
          requestHandler.submit((SubmitRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        }
      }
    }
  };

  public VertxEventBusServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public void requestHandler(RequestHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().registerHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.eventBus().unregisterHandler(address, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      }
    });
    return future;
  }

}
