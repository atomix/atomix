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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.kuujo.copycat.Arguments;
import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.InstallSnapshotRequest;
import net.kuujo.copycat.protocol.InstallSnapshotResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
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
  private final Vertx vertx;
  private final String host;
  private final int port;
  private NetServer server;
  private ProtocolHandler requestHandler;

  public TcpProtocolServer(Vertx vertx, String host, int port) {
    this.vertx = vertx;
    this.host = host;
    this.port = port;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    if (server == null) {
      server = vertx.createNetServer();
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
                  case "install":
                    handleInstallRequest(socket, request);
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
            callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
          } else {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      });
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

  /**
   * Handles an append entries request.
   */
  private void handleAppendRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      List<Entry> entries = new ArrayList<>();
      JsonArray jsonEntries = request.getArray("entries");
      if (jsonEntries != null) {
        for (Object jsonEntry : jsonEntries) {
          entries.add(serializer.readValue(jsonEntry.toString().getBytes(), Entry.class));
        }
      }
      requestHandler.appendEntries(new AppendEntriesRequest(request.getLong("term"), request.getString("leader"), request.getLong("prevIndex"), request.getLong("prevTerm"), entries, request.getLong("commit")), new AsyncCallback<AppendEntriesResponse>() {
        @Override
        public void call(net.kuujo.copycat.AsyncResult<AppendEntriesResponse> result) {
          if (result.succeeded()) {
            AppendEntriesResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
            } else {
              respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Handles an install request.
   */
  private void handleInstallRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      Set<String> cluster = new HashSet<>();
      JsonArray jsonNodes = request.getArray("cluster");
      if (jsonNodes != null) {
        for (Object jsonNode : jsonNodes) {
          cluster.add(jsonNode.toString());
        }
      }
      requestHandler.installSnapshot(new InstallSnapshotRequest(request.getLong("term"), request.getString("leader"), request.getLong("snapshotIndex"), request.getLong("snapshotTerm"), cluster, request.getBinary("data"), request.getBoolean("complete")), new AsyncCallback<InstallSnapshotResponse>() {
        @Override
        public void call(net.kuujo.copycat.AsyncResult<InstallSnapshotResponse> result) {
          if (result.succeeded()) {
            InstallSnapshotResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("succeeded", response.succeeded()));
            } else {
              respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Handles a vote request.
   */
  private void handleVoteRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      requestHandler.requestVote(new RequestVoteRequest(request.getLong("term"), request.getString("candidate"), request.getLong("lastIndex"), request.getLong("lastTerm")), new AsyncCallback<RequestVoteResponse>() {
        @Override
        public void call(net.kuujo.copycat.AsyncResult<RequestVoteResponse> result) {
          if (result.succeeded()) {
            RequestVoteResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putNumber("term", response.term()).putBoolean("voteGranted", response.voteGranted()));
            } else {
              respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", result.cause().getMessage()));
          }
        }
      });
    }
  }

  /**
   * Handles a submit request.
   */
  private void handleSubmitRequest(final NetSocket socket, JsonObject request) {
    if (requestHandler != null) {
      final long id = request.getLong("id");
      requestHandler.submitCommand(new SubmitCommandRequest(request.getString("command"), new Arguments(request.getObject("args").toMap())), new AsyncCallback<SubmitCommandResponse>() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void call(net.kuujo.copycat.AsyncResult<SubmitCommandResponse> result) {
          if (result.succeeded()) {
            SubmitCommandResponse response = result.value();
            if (response.status().equals(Response.Status.OK)) {
              if (response.result() instanceof Map) {
                respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putObject("result", new JsonObject((Map) response.result())));
              } else if (response.result() instanceof List) {
                respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putArray("result", new JsonArray((List) response.result())));
              } else {
                respond(socket, new JsonObject().putString("status", "ok").putNumber("id", id).putValue("result", response.result()));
              }
            } else {
              respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", response.error().getMessage()));
            }
          } else {
            respond(socket, new JsonObject().putString("status", "error").putNumber("id", id).putString("message", result.cause().getMessage()));
          }
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
  public void stop(final AsyncCallback<Void> callback) {
    if (server != null) {
      server.close(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
          } else {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      });
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

}
