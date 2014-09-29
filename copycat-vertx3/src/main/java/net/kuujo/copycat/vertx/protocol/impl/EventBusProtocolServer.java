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
package net.kuujo.copycat.vertx.protocol.impl;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolReader;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.ProtocolWriter;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.SubmitCommandRequest;

/**
 * Vert.x event bus protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolServer implements ProtocolServer {
  private final ProtocolReader reader = new ProtocolReader();
  private final ProtocolWriter writer = new ProtocolWriter();
  private final String address;
  private Vertx vertx;
  private ProtocolHandler requestHandler;

  private final Handler<Message<byte[]>> messageHandler = new Handler<Message<byte[]>>() {
    @Override
    public void handle(Message<byte[]> message) {
      if (requestHandler != null) {
        Request request = reader.readRequest(message.body());
        if (request instanceof AppendEntriesRequest) {
          requestHandler.appendEntries((AppendEntriesRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        } else if (request instanceof RequestVoteRequest) {
          requestHandler.requestVote((RequestVoteRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        } else if (request instanceof SubmitCommandRequest) {
          requestHandler.submitCommand((SubmitCommandRequest) request).whenComplete((response, error) -> {
            message.reply(writer.writeResponse(response));
          });
        }
      }
    }
  };

  public EventBusProtocolServer(String address, Vertx vertx) {
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public CompletableFuture<Void> start() {
    vertx.eventBus().registerHandler(address, messageHandler);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

}
