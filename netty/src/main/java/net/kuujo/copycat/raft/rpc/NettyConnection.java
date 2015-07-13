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
package net.kuujo.copycat.raft.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import net.kuujo.copycat.util.Context;
import net.openhft.hashing.LongHashFunction;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Netty connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyConnection implements Connection {
  static final byte CONNECT = 0x00;
  static final byte REQUEST = 0x01;
  static final byte RESPONSE = 0x02;
  static final byte SUCCESS = 0x03;
  static final byte FAILURE = 0x04;
  private static final ThreadLocal<ByteBufBuffer> BUFFER = new ThreadLocal<ByteBufBuffer>() {
    @Override
    protected ByteBufBuffer initialValue() {
      return new ByteBufBuffer();
    }
  };

  private final int id;
  private final Channel channel;
  protected final Context context;
  private final Map<Integer, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Map<Class, Integer> hashMap = new HashMap<>();
  private final LongHashFunction hash = LongHashFunction.city_1_1();
  private ExceptionListener exceptionListener;
  private CloseListener closeListener;
  private long requestId;
  private final Map<Long, ContextualFuture> responseFutures = new LinkedHashMap<>(1024);
  private ChannelFuture writeFuture;

  public NettyConnection(int id, Channel channel, Context context) {
    this.id = id;
    this.channel = channel;
    this.context = context;
  }

  /**
   * Returns the current execution context.
   */
  private Context getContext() {
    Context context = Context.currentContext();
    return context != null ? context : this.context;
  }

  @Override
  public int id() {
    return id;
  }

  /**
   * Hashes the given string to a 32-bit hash.
   */
  private int hash32(Class type) {
    long hash = this.hash.hashChars(type.getName());
    return (int)(hash ^ (hash >>> 32));
  }

  /**
   * Handles a request.
   */
  void handleRequest(ByteBuf request) {
    long requestId = request.readLong();
    int address = request.readInt();
    HandlerHolder handler = handlers.get(address);
    if (handler != null) {
      handler.context.execute(() -> {
        handleRequest(requestId, readRequest(request), handler);
        request.release();
      });
    }
  }

  /**
   * Handles a request.
   */
  private void handleRequest(long requestId, Request request, HandlerHolder handler) {
    @SuppressWarnings("unchecked")
    CompletableFuture<Response> responseFuture = handler.handler.handle(request);
    responseFuture.whenCompleteAsync((response, error) -> {
      if (error == null) {
        handleRequestSuccess(requestId, response);
      } else {
        handleRequestFailure(requestId, error);
      }
      request.release();
    }, channel.eventLoop());
  }

  /**
   * Handles a request response.
   */
  private void handleRequestSuccess(long requestId, Response response) {
    ByteBuf buffer = channel.alloc().buffer(9, 1024 * 8);
    buffer.writeLong(requestId);
    buffer.writeByte(SUCCESS);
    writeFuture = channel.write(writeResponse(buffer, response), channel.voidPromise());
    response.release();
  }

  /**
   * Handles a request failure.
   */
  private void handleRequestFailure(long requestId, Throwable error) {
    ByteBuf buffer = channel.alloc().buffer(9, 1024 * 8);
    buffer.writeLong(requestId);
    buffer.writeByte(FAILURE);
    writeFuture = channel.write(writeError(buffer, error), channel.voidPromise());
  }

  /**
   * Handles response.
   */
  void handleResponse(ByteBuf response) {
    long requestId = response.readLong();
    byte status = response.readByte();
    switch (status) {
      case SUCCESS:
        handleResponseSuccess(requestId, readResponse(response));
        break;
      case FAILURE:
        handleResponseFailure(requestId, readError(response));
        break;
    }
    response.release();
  }

  /**
   * Handles a successful response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponseSuccess(long requestId, Response response) {
    ContextualFuture future = responseFutures.get(requestId);
    if (future != null) {
      future.context.execute(() -> {
        future.complete(response);
        response.release();
      });
    }
  }

  /**
   * Handles a failure response.
   */
  private void handleResponseFailure(long requestId, Throwable t) {
    ContextualFuture future = responseFutures.get(requestId);
    if (future != null) {
      future.context.execute(() -> {
        future.completeExceptionally(t);
      });
    }
  }

  /**
   * Writes a request to the given buffer.
   */
  private ByteBuf writeRequest(ByteBuf buffer, Request request) {
    Context context = getContext();
    ByteBufBuffer requestBuffer = BUFFER.get();
    requestBuffer.setByteBuf(buffer);
    context.serializer().writeObject(request, requestBuffer);
    return buffer;
  }

  /**
   * Writes a response to the given buffer.
   */
  private ByteBuf writeResponse(ByteBuf buffer, Response request) {
    Context context = getContext();
    ByteBufBuffer responseBuffer = BUFFER.get();
    responseBuffer.setByteBuf(buffer);
    context.serializer().writeObject(request, responseBuffer);
    return buffer;
  }

  /**
   * Writes an error to the given buffer.
   */
  private ByteBuf writeError(ByteBuf buffer, Throwable t) {
    Context context = getContext();
    ByteBufBuffer requestBuffer = BUFFER.get();
    requestBuffer.setByteBuf(buffer);
    context.serializer().writeObject(t, requestBuffer);
    return buffer;
  }

  /**
   * Reads a request from the given buffer.
   */
  private Request readRequest(ByteBuf buffer) {
    Context context = getContext();
    ByteBufBuffer requestBuffer = BUFFER.get();
    requestBuffer.setByteBuf(buffer);
    return context.serializer().readObject(requestBuffer);
  }

  /**
   * Reads a response from the given buffer.
   */
  private Response readResponse(ByteBuf buffer) {
    ByteBufBuffer responseBuffer = BUFFER.get();
    responseBuffer.setByteBuf(buffer);
    return context.serializer().readObject(responseBuffer);
  }

  /**
   * Reads an error from the given buffer.
   */
  private Throwable readError(ByteBuf buffer) {
    ByteBufBuffer responseBuffer = BUFFER.get();
    responseBuffer.setByteBuf(buffer);
    return context.serializer().readObject(responseBuffer);
  }

  /**
   * Handles an exception.
   *
   * @param t The exception to handle.
   */
  void handleException(Throwable t) {
    if (exceptionListener != null) {
      exceptionListener.exception(t);
    }
  }

  /**
   * Handles the channel being closed.
   */
  void handleClosed() {
    if (closeListener != null) {
      closeListener.closed(this);
    }
  }

  @Override
  public <T extends Request<T>, U extends Response<U>> CompletableFuture<U> send(T request) {
    Context context = getContext();
    ContextualFuture<U> future = new ContextualFuture<>(context);

    long requestId = ++this.requestId;

    ByteBuf buffer = this.channel.alloc().buffer(13, 1024 * 32);
    buffer.writeLong(requestId)
      .writeByte(REQUEST)
      .writeInt(hashMap.computeIfAbsent(request.getClass(), this::hash32));

    writeFuture = channel.writeAndFlush(writeRequest(buffer, request)).addListener((channelFuture) -> {
      if (channelFuture.isSuccess()) {
        responseFutures.put(requestId, future);
      } else {
        future.context.execute(() -> {
          future.completeExceptionally(new RpcException(channelFuture.cause()));
        });
      }
    });
    return future;
  }

  @Override
  public <T extends Request<T>, U extends Response<U>> Connection handler(Class<T> type, RequestHandler<T, U> handler) {
    handlers.put(hashMap.computeIfAbsent(type, this::hash32), new HandlerHolder(handler, getContext()));
    return null;
  }

  @Override
  public Connection exceptionListener(ExceptionListener listener) {
    exceptionListener = listener;
    return this;
  }

  @Override
  public Connection closeListener(CloseListener listener) {
    closeListener = listener;
    return this;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (writeFuture != null) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    } else {
      channel.close();
    }
    return null;
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final RequestHandler handler;
    private final Context context;

    private HandlerHolder(RequestHandler handler, Context context) {
      this.handler = handler;
      this.context = context;
    }
  }

  /**
   * Contextual future.
   */
  private static class ContextualFuture<T> extends CompletableFuture<T> {
    private final Context context;

    private ContextualFuture(Context context) {
      this.context = context;
    }
  }

}
