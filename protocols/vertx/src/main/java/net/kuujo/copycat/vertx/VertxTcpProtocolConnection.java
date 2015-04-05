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
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

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
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private final Vertx vertx;
  private final NetSocket socket;
  private ProtocolHandler handler;
  private final Map<Object, ResponseHolder> responses = new HashMap<>(1000);
  private long requestId;

  /**
   * Holder for response handlers.
   */
  private static class ResponseHolder {
    private final CompletableFuture<Buffer> future;
    private final long timer;
    private ResponseHolder(long timerId, CompletableFuture<Buffer> future) {
      this.timer = timerId;
      this.future = future;
    }
  }

  public VertxTcpProtocolConnection(Vertx vertx, NetSocket socket) {
    this.vertx = vertx;
    this.socket = socket;
    RecordParser parser = RecordParser.newFixed(4, null);
    Handler<org.vertx.java.core.buffer.Buffer> handler = new Handler<org.vertx.java.core.buffer.Buffer>() {
      int length = -1;
      @Override
      public void handle(org.vertx.java.core.buffer.Buffer buffer) {
        if (length == -1) {
          length = buffer.getInt(0);
          parser.fixedSizeMode(length + 8);
        } else {
          handleMessage(buffer.getByte(0), buffer.getLong(1), bufferPool.acquire().write(buffer.getBytes(9, length + 9)).flip());
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
  public CompletableFuture<Buffer> write(Buffer request) {
    CompletableFuture<Buffer> future = new CompletableFuture<>();
    long requestId = this.requestId++;
    byte[] bytes = new byte[(int) request.remaining()];
    request.read(bytes);
    socket.write(new org.vertx.java.core.buffer.Buffer()
      .appendByte(REQUEST)
      .appendInt(bytes.length)
      .appendLong(requestId)
      .appendBytes(bytes));
    storeFuture(requestId, future);
    return future;
  }

  /**
   * Handles a request or response.
   */
  private void handleMessage(byte type, long id, Buffer message) {
    if (type == REQUEST) {
      handleRequest(id, message);
    } else if (type == RESPONSE) {
      handleResponse(id, message);
    }
  }

  /**
   * Handles a request.
   */
  private void handleRequest(final long id, final Buffer request) {
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
  private void handleResponse(long id, Buffer response) {
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
  private void storeFuture(final long id, CompletableFuture<Buffer> future) {
    long timerId = vertx.setTimer(5000, timer -> {
      handleError(id, new ProtocolException("Request timed out"));
    });
    ResponseHolder holder = new ResponseHolder(timerId, future);
    responses.put(id, holder);
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, long id, Buffer response) {
    int length = (int) response.remaining();
    byte[] bytes = new byte[length];
    response.read(bytes);
    socket.write(new org.vertx.java.core.buffer.Buffer().appendByte(RESPONSE).appendInt(length).appendLong(id).appendBytes(bytes));
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
