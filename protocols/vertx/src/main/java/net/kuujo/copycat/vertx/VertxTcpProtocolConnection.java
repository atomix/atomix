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

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolConnection implements ProtocolConnection {
  private static final byte REQUEST = 0;
  private static final byte RESPONSE = 1;
  private final Vertx vertx;
  private final NetSocket socket;
  private ProtocolHandler handler;
  private final Map<Object, ResponseHolder> responses = new HashMap<>(1000);
  private long requestId;

  /**
   * Holder for response handlers.
   */
  private static class ResponseHolder {
    private final CompletableFuture<ByteBuffer> future;
    private final long timer;
    private ResponseHolder(long timerId, CompletableFuture<ByteBuffer> future) {
      this.timer = timerId;
      this.future = future;
    }
  }

  public VertxTcpProtocolConnection(Vertx vertx, NetSocket socket) {
    this.vertx = vertx;
    this.socket = socket;
    RecordParser parser = RecordParser.newFixed(4, null);
    Handler<Buffer> handler = new Handler<Buffer>() {
      int length = -1;
      @Override
      public void handle(Buffer buffer) {
        if (length == -1) {
          length = buffer.getInt(0);
          parser.fixedSizeMode(length + 8);
        } else {
          handleMessage(buffer.getByte(0), buffer.getLong(1), buffer.getBuffer(9, length + 9).getByteBuf().nioBuffer());
          length = -1;
          parser.fixedSizeMode(4);
        }
      }
    };
    parser.setOutput(handler);
    socket.dataHandler(parser);
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    if (socket != null) {
      long requestId = this.requestId++;
      byte[] bytes = new byte[request.remaining()];
      request.get(bytes);
      socket.write(new Buffer().appendByte(REQUEST).appendInt(bytes.length).appendLong(requestId).appendBytes(bytes));
      storeFuture(requestId, future);
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  /**
   * Handles a request or response.
   */
  private void handleMessage(byte type, long id, ByteBuffer message) {
    if (type == REQUEST) {
      handleRequest(id, message);
    } else if (type == RESPONSE) {
      handleResponse(id, message);
    }
  }

  /**
   * Handles a request.
   */
  private void handleRequest(final long id, final ByteBuffer request) {
    if (handler != null) {
      handler.apply(request).whenComplete((response, error) -> {
        if (error == null) {
          respond(socket, id, response);
        }
      });
    }
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(long id, ByteBuffer response) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      holder.future.complete(response);
    }
  }

  /**
   * Handles an identifiable error.
   */
  private void handleError(long id, Throwable error) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      holder.future.completeExceptionally(error);
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private void storeFuture(final long id, CompletableFuture<ByteBuffer> future) {
    long timerId = vertx.setTimer(5000, timer -> {
      handleError(id, new ProtocolException("Request timed out"));
    });
    ResponseHolder holder = new ResponseHolder(timerId, future);
    responses.put(id, holder);
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, long id, ByteBuffer response) {
    int length = response.remaining();
    byte[] bytes = new byte[length];
    response.get(bytes);
    socket.write(new Buffer().appendByte(RESPONSE).appendInt(length).appendLong(id).appendBytes(bytes));
  }

  @Override
  public ProtocolConnection closeListener(EventListener<Void> listener) {
    socket.closeHandler(listener::accept);
    return this;
  }

  @Override
  public ProtocolConnection exceptionListener(EventListener<Throwable> listener) {
    socket.exceptionHandler(listener::accept);
    return this;
  }

  @Override
  public CompletableFuture<Void> close() {
    socket.close();
    return CompletableFuture.completedFuture(null);
  }

}
