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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

/**
 * Vert.x TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocolServer implements ProtocolServer {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private Vertx vertx;
  private final String host;
  private final int port;
  private boolean clientAuthRequired;
  private final TcpProtocol protocol;
  private NetServer server;
  private ProtocolHandler requestHandler;

  public TcpProtocolServer(String host, int port, TcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   */
  public void setClientAuthRequired(boolean required) {
    this.clientAuthRequired = required;
  }

  /**
   * Returns whether client authentication is required.
   *
   * @return Whether client authentication is required.
   */
  public boolean isClientAuthRequired() {
    return clientAuthRequired;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   * @return The TCP protocol.
   */
  public TcpProtocolServer withClientAuthRequired(boolean required) {
    this.clientAuthRequired = required;
    return this;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public CompletableFuture<Void> start() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (vertx == null) {
      vertx = new DefaultVertx();
    }

    if (server == null) {
      server = vertx.createNetServer();
      server.setTCPKeepAlive(true);
      server.setTCPNoDelay(true);
      server.setReuseAddress(true);
      server.setAcceptBacklog(protocol.getAcceptBacklog());
      server.setSendBufferSize(protocol.getSendBufferSize());
      server.setReceiveBufferSize(protocol.getReceiveBufferSize());
      server.setSSL(protocol.isSsl());
      server.setKeyStorePath(protocol.getKeyStorePath());
      server.setKeyStorePassword(protocol.getKeyStorePassword());
      server.setTrustStorePath(protocol.getTrustStorePath());
      server.setTrustStorePassword(protocol.getTrustStorePassword());
      server.setClientAuthRequired(clientAuthRequired);
      server.setUsePooledBuffers(true);
      server.connectHandler(new Handler<NetSocket>() {
        @Override
        public void handle(final NetSocket socket) {
          socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
              JsonObject request = new JsonObject(buffer.toString());
              String type = request.getString("type");
              if (type != null) {
                switch (type) {
                  case "append":
                    handleAppendRequest(socket, request);
                    break;
                  case "vote":
                    handleVoteRequest(socket, request);
                    break;
                  case "submit":
                    handleSubmitRequest(socket, request);
                    break;
                  default:
                    respond(socket, new JsonObject().putString("status", "error").putString("message", "Invalid request type"));
                    break;
                }
              } else {
                respond(socket, new JsonObject().putString("status", "error").putString("message", "Invalid request type"));
              }
            }
          }));
        }
      }).listen(port, host, new Handler<AsyncResult<NetServer>>() {
        @Override
        public void handle(AsyncResult<NetServer> result) {
          if (result.failed()) {
            future.completeExceptionally(result.cause());
          } else {
            future.complete(null);
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Handles an append entries request.
   */
  private void handleAppendRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = request.getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      requestHandler.appendEntries(new AppendEntriesRequest(id, request.getLong("term"), request.getString("leader"), request.getLong("prevIndex"), request.getLong("prevTerm"), entries, request.getLong("commit"))).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()).putNumber("lastIndex", response.lastLogIndex()));
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a vote request.
   */
  private void handleVoteRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      requestHandler.requestVote(new RequestVoteRequest(id, request.getLong("term"), request.getString("candidate"), request.getLong("lastIndex"), request.getLong("lastTerm"))).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
        }
      });
    }
  }

  /**
   * Handles a submit request.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void handleSubmitRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final Object id = request.getValue("id");
      requestHandler.submitCommand(new SubmitCommandRequest(id, request.getString("command"), request.getArray("args").toList())).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status().equals(Response.Status.OK)) {
            if (response.result() instanceof Map) {
              respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putObject("result", new JsonObject((Map) response.result())));
            } else if (response.result() instanceof List) {
              respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putArray("result", new JsonArray((List) response.result())));
            } else {
              respond(socket, new JsonObject().putString("status", "ok").putValue("id", id).putValue("result", response.result()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", response.error().getMessage()));
          }
        } else {
          respond(socket, new JsonObject().putString("status", "error").putValue("id", id).putString("message", error.getMessage()));
        }
      });
    }
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, JsonObject response) {
    socket.write(response.encode() + '\00');
  }

  @Override
  public CompletableFuture<Void> stop() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (server != null) {
      server.close(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            future.completeExceptionally(result.cause());
          } else {
            future.complete(null);
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

}
