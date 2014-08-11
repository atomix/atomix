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

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.InstallSnapshotRequest;
import net.kuujo.copycat.protocol.InstallSnapshotResponse;
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
  private final Vertx vertx;
  private final String host;
  private final int port;
  private NetClient client;
  private NetSocket socket;
  private final Map<Object, ResponseHolder> responses = new HashMap<>();

  /**
   * Holder for response handlers.
   */
  @SuppressWarnings("rawtypes")
  private static class ResponseHolder {
    private final AsyncCallback callback;
    private final ResponseType type;
    private final long timer;
    private ResponseHolder(long timerId, ResponseType type, AsyncCallback callback) {
      this.timer = timerId;
      this.type = type;
      this.callback = callback;
    }
  }

  /**
   * Indicates response types.
   */
  private static enum ResponseType {
    APPEND,
    INSTALL,
    VOTE,
    SUBMIT;
  }

  public TcpProtocolClient(Vertx vertx, String host, int port) {
    this.vertx = vertx;
    this.host = host;
    this.port = port;
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback) {
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
      storeCallback(request.id(), ResponseType.APPEND, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void installSnapshot(InstallSnapshotRequest request, AsyncCallback<InstallSnapshotResponse> callback) {
    if (socket != null) {
      JsonArray jsonCluster = new JsonArray();
      for (String member : request.cluster()) {
        jsonCluster.addString(member);
      }
      socket.write(new JsonObject().putString("type", "install")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putArray("cluster", jsonCluster)
          .putBinary("data", request.data())
          .putBoolean("complete", request.complete())
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.INSTALL, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<InstallSnapshotResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "vote")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("candidate", request.candidate())
          .putNumber("lastIndex", request.lastLogIndex())
          .putNumber("lastTerm", request.lastLogTerm())
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.VOTE, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "submit")
          .putValue("id", request.id())
          .putString("command", request.command())
          .putObject("args", new JsonObject(request.args()))
          .encode() + '\00');
      storeCallback(request.id(), ResponseType.SUBMIT, callback);
    } else {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException("Client not connected")));
    }
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(String id, JsonObject response) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      switch (holder.type) {
        case APPEND:
          handleAppendResponse(response, (AsyncCallback<AppendEntriesResponse>) holder.callback);
          break;
        case INSTALL:
          handleInstallResponse(response, (AsyncCallback<InstallSnapshotResponse>) holder.callback);
          break;
        case VOTE:
          handleVoteResponse(response, (AsyncCallback<RequestVoteResponse>) holder.callback);
          break;
        case SUBMIT:
          handleSubmitResponse(response, (AsyncCallback<SubmitCommandResponse>) holder.callback);
          break;
      }
    }
  }

  /**
   * Handles an append entries response.
   */
  private void handleAppendResponse(JsonObject response, AsyncCallback<AppendEntriesResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new AppendEntriesResponse(response.getString("id"), response.getLong("term"), response.getBoolean("succeeded"))));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<AppendEntriesResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Handles an install response.
   */
  private void handleInstallResponse(JsonObject response, AsyncCallback<InstallSnapshotResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<InstallSnapshotResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<InstallSnapshotResponse>(new InstallSnapshotResponse(response.getString("id"), response.getLong("term"), response.getBoolean("succeeded"))));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<InstallSnapshotResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Handles a vote response.
   */
  private void handleVoteResponse(JsonObject response, AsyncCallback<RequestVoteResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new RequestVoteResponse(response.getString("id"), response.getLong("term"), response.getBoolean("voteGranted"))));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<RequestVoteResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Handles a submit response.
   */
  private void handleSubmitResponse(JsonObject response, AsyncCallback<SubmitCommandResponse> callback) {
    String status = response.getString("status");
    if (status == null) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException("Invalid response")));
    } else if (status.equals("ok")) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new SubmitCommandResponse(response.getString("id"), response.getObject("result").toMap())));
    } else if (status.equals("error")) {
      callback.call(new net.kuujo.copycat.AsyncResult<SubmitCommandResponse>(new ProtocolException(response.getString("message"))));
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private <T extends Response> void storeCallback(final Object id, ResponseType responseType, AsyncCallback<T> callback) {
    long timerId = vertx.setTimer(30000, new Handler<Long>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(Long timerID) {
        ResponseHolder holder = responses.remove(id);
        if (holder != null) {
          holder.callback.call(new net.kuujo.copycat.AsyncResult<T>(new ProtocolException("Request timed out")));
        }
      }
    });
    ResponseHolder holder = new ResponseHolder(timerId, responseType, callback);
    responses.put(id, holder);
  }

  @Override
  public void connect() {
    connect(null);
  }

  @Override
  public void connect(final AsyncCallback<Void> callback) {
    if (client == null) {
      client = vertx.createNetClient();
      client.connect(port, host, new Handler<AsyncResult<NetSocket>>() {
        @Override
        public void handle(AsyncResult<NetSocket> result) {
          if (result.failed()) {
            if (callback != null) {
              callback.call(new net.kuujo.copycat.AsyncResult<Void>(result.cause()));
            }
          } else {
            socket = result.result();
            socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                JsonObject response = new JsonObject(buffer.toString());
                String id = response.getString("id");
                handleResponse(id, response);
              }
            }));
            if (callback != null) {
              callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
            }
          }
        }
      });
    } else if (callback != null) {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

  @Override
  public void close() {
    close(null);
  }

  @Override
  public void close(final AsyncCallback<Void> callback) {
    if (client != null && socket != null) {
      socket.closeHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
          socket = null;
          client.close();
          client = null;
          if (callback != null) {
            callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
          }
        }
      }).close();
    } else if (client != null) {
      client.close();
      client = null;
      if (callback != null) {
        callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
      }
    } else if (callback != null) {
      callback.call(new net.kuujo.copycat.AsyncResult<Void>((Void) null));
    }
  }

}
