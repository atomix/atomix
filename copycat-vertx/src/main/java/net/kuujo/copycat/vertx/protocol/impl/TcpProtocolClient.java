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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

/**
 * Vert.x TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocolClient implements ProtocolClient {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private Vertx vertx;
  private final String host;
  private final int port;
  private boolean trustAll;
  private final TcpProtocol protocol;
  private NetClient client;
  private NetSocket socket;
  private final Map<Object, ResponseHolder> responses = new HashMap<>();

  /**
   * Holder for response handlers.
   */
  @SuppressWarnings("rawtypes")
  private static class ResponseHolder {
    private final CompletableFuture future;
    private final ResponseType type;
    private final long timer;
    private ResponseHolder(long timerId, ResponseType type, CompletableFuture future) {
      this.timer = timerId;
      this.type = type;
      this.future = future;
    }
  }

  /**
   * Indicates response types.
   */
  private static enum ResponseType {
    APPEND,
    VOTE,
    SUBMIT;
  }

  public TcpProtocolClient(String host, int port, TcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  /**
   * Sets whether to trust all server certs.
   *
   * @param trustAll Whether to trust all server certs.
   */
  public void setTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
  }

  /**
   * Returns whether to trust all server certs.
   *
   * @return Whether to trust all server certs.
   */
  public boolean isTrustAll() {
    return trustAll;
  }

  /**
   * Sets whether to trust all server certs, returning the protocol for method chaining.
   *
   * @param trustAll Whether to trust all server certs.
   * @return The TCP protocol.
   */
  public TcpProtocolClient withTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
    return this;
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    final CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
    if (socket != null) {
      JsonArray jsonEntries = new JsonArray();
      for (Entry entry : request.entries()) {
        jsonEntries.addString(new String(serializer.writeValue(entry)));
      }
      socket.write(new JsonObject().putString("type", "append")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putNumber("prevIndex", request.prevLogIndex())
          .putNumber("prevTerm", request.prevLogTerm())
          .putArray("entries", jsonEntries)
          .putNumber("commit", request.commitIndex()).encode() + '\00');
      storeCallback(request.id(), ResponseType.APPEND, future);
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    final CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "vote")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("candidate", request.candidate())
          .putNumber("lastIndex", request.lastLogIndex())
          .putNumber("lastTerm", request.lastLogTerm())
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.VOTE, future);
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    final CompletableFuture<SubmitCommandResponse> future = new CompletableFuture<>();
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "submit")
          .putValue("id", request.id())
          .putString("command", request.command())
          .putArray("args", new JsonArray(request.args()))
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.SUBMIT, future);
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(Object id, JsonObject response) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      switch (holder.type) {
        case APPEND:
          handleAppendResponse(response, (CompletableFuture<AppendEntriesResponse>) holder.future);
          break;
        case VOTE:
          handleVoteResponse(response, (CompletableFuture<RequestVoteResponse>) holder.future);
          break;
        case SUBMIT:
          handleSubmitResponse(response, (CompletableFuture<SubmitCommandResponse>) holder.future);
          break;
      }
    }
  }

  /**
   * Handles an append entries response.
   */
  private void handleAppendResponse(JsonObject response, CompletableFuture<AppendEntriesResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.completeExceptionally(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      future.complete(new AppendEntriesResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("succeeded"), response.getLong("lastIndex")));
    } else if (status.equals("error")) {
      future.completeExceptionally(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a vote response.
   */
  private void handleVoteResponse(JsonObject response, CompletableFuture<RequestVoteResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.completeExceptionally(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      future.complete(new RequestVoteResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("voteGranted")));
    } else if (status.equals("error")) {
      future.completeExceptionally(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a submit response.
   */
  private void handleSubmitResponse(JsonObject response, CompletableFuture<SubmitCommandResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.completeExceptionally(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      Object result = response.getValue("result");
      if (result instanceof JsonArray) {
        future.complete(new SubmitCommandResponse(response.getValue("id"), ((JsonArray) result).toList()));
      } else if (result instanceof JsonObject) {
        future.complete(new SubmitCommandResponse(response.getValue("id"), ((JsonObject) result).toMap()));
      } else {
        future.complete(new SubmitCommandResponse(response.getValue("id"), result));
      }
    } else if (status.equals("error")) {
      future.completeExceptionally(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private <T extends Response> void storeCallback(final Object id, ResponseType responseType, CompletableFuture<T> future) {
    long timerId = vertx.setTimer(30000, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        ResponseHolder holder = responses.remove(id);
        if (holder != null) {
          holder.future.completeExceptionally(new ProtocolException("Request timed out"));
        }
      }
    });
    ResponseHolder holder = new ResponseHolder(timerId, responseType, future);
    responses.put(id, holder);
  }

  @Override
  public CompletableFuture<Void> connect() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (vertx == null) {
      vertx = new DefaultVertx();
    }

    if (client == null) {
      client = vertx.createNetClient();
      client.setTCPKeepAlive(true);
      client.setTCPNoDelay(true);
      client.setSendBufferSize(protocol.getSendBufferSize());
      client.setReceiveBufferSize(protocol.getReceiveBufferSize());
      client.setSSL(protocol.isSsl());
      client.setKeyStorePath(protocol.getKeyStorePath());
      client.setKeyStorePassword(protocol.getKeyStorePassword());
      client.setTrustStorePath(protocol.getTrustStorePath());
      client.setTrustStorePassword(protocol.getTrustStorePassword());
      client.setTrustAll(trustAll);
      client.setUsePooledBuffers(true);
      client.connect(port, host, new Handler<AsyncResult<NetSocket>>() {
        @Override
        public void handle(AsyncResult<NetSocket> result) {
          if (result.failed()) {
            future.completeExceptionally(result.cause());
          } else {
            socket = result.result();
            socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                JsonObject response = new JsonObject(buffer.toString());
                Object id = response.getValue("id");
                handleResponse(id, response);
              }
            }));
            future.complete(null);
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (client != null && socket != null) {
      socket.closeHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
          socket = null;
          client.close();
          client = null;
          future.complete(null);
        }
      }).close();
    } else if (client != null) {
      client.close();
      client = null;
      future.complete(null);
    } else {
      future.complete(null);
    }
    return future;
  }

}
