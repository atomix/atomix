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
package net.kuujo.copycat.io.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.ReferenceCounted;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.Listener;
import net.kuujo.copycat.util.Listeners;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Scheduled;
import net.openhft.hashing.LongHashFunction;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Netty connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyConnection implements Connection {
  static final byte CONNECT = 0x00;
  static final byte OK = 0x01;
  static final byte REQUEST = 0x02;
  static final byte RESPONSE = 0x03;
  static final byte SUCCESS = 0x04;
  static final byte FAILURE = 0x05;
  private static final long REQUEST_TIMEOUT = 500;
  private static final ThreadLocal<ByteBufInput> INPUT = new ThreadLocal<ByteBufInput>() {
    @Override
    protected ByteBufInput initialValue() {
      return new ByteBufInput();
    }
  };
  private static final ThreadLocal<ByteBufOutput> OUTPUT = new ThreadLocal<ByteBufOutput>() {
    @Override
    protected ByteBufOutput initialValue() {
      return new ByteBufOutput();
    }
  };

  private final UUID id;
  private final Channel channel;
  private final Context context;
  private final Map<Integer, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Map<Class, Integer> hashMap = new HashMap<>();
  private final LongHashFunction hash = LongHashFunction.city_1_1();
  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();
  private volatile long requestId;
  private volatile Throwable failure;
  private volatile boolean closed;
  private Scheduled timeout;
  private final Map<Long, ContextualFuture> responseFutures = new LinkedHashMap<>(1024);
  private ChannelFuture writeFuture;

  /**
   * @throws NullPointerException if any argument is null
   */
  public NettyConnection(UUID id, Channel channel, Context context) {
    this.id = id;
    this.channel = channel;
    this.context = context;
    this.timeout = context.schedule(this::timeout, Duration.ofMillis(250));
  }

  /**
   * Returns the current execution context.
   */
  private Context getContext() {
    Context context = Context.currentContext();
    Assert.state(context != null, "not on a Copycat thread");
    return context;
  }

  @Override
  public UUID id() {
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
  void handleRequest(ByteBuf buffer) {
    long requestId = buffer.readLong();
    int address = buffer.readInt();
    HandlerHolder handler = handlers.get(address);
    if (handler != null) {
      Object request = readRequest(buffer);
      handler.context.executor().execute(() -> {
        handleRequest(requestId, request, handler);

        if (request instanceof ReferenceCounted) {
          ((ReferenceCounted) request).release();
        }
      });
    }
  }

  /**
   * Handles a request.
   */
  private void handleRequest(long requestId, Object request, HandlerHolder handler) {
    @SuppressWarnings("unchecked")
    CompletableFuture<Object> responseFuture = handler.handler.handle(request);
    responseFuture.whenCompleteAsync((response, error) -> {
      if (error == null) {
        handleRequestSuccess(requestId, response);
      } else {
        handleRequestFailure(requestId, error);
      }

      if (request instanceof ReferenceCounted) {
        ((ReferenceCounted) request).release();
      }
    }, context.executor());
  }

  /**
   * Handles a request response.
   */
  private void handleRequestSuccess(long requestId, Object response) {
    ByteBuf buffer = channel.alloc().buffer(10)
      .writeByte(RESPONSE)
      .writeLong(requestId)
      .writeByte(SUCCESS);
    channel.writeAndFlush(writeResponse(buffer, response), channel.voidPromise());

    if (response instanceof ReferenceCounted) {
      ((ReferenceCounted) response).release();
    }
  }

  /**
   * Handles a request failure.
   */
  private void handleRequestFailure(long requestId, Throwable error) {
    ByteBuf buffer = channel.alloc().buffer(10)
      .writeByte(RESPONSE)
      .writeLong(requestId)
      .writeByte(FAILURE);
    channel.writeAndFlush(writeError(buffer, error), channel.voidPromise());
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
  private void handleResponseSuccess(long requestId, Object response) {
    ContextualFuture future = responseFutures.remove(requestId);
    if (future != null) {
      future.context.executor().execute(() -> {
        future.complete(response);

        if (response instanceof ReferenceCounted) {
          ((ReferenceCounted) response).release();
        }
      });
    }
  }

  /**
   * Handles a failure response.
   */
  private void handleResponseFailure(long requestId, Throwable t) {
    ContextualFuture future = responseFutures.remove(requestId);
    if (future != null) {
      future.context.executor().execute(() -> future.completeExceptionally(t));
    }
  }

  /**
   * Writes a request to the given buffer.
   */
  private ByteBuf writeRequest(ByteBuf buffer, Object request) {
    context.serializer().writeObject(request, OUTPUT.get().setByteBuf(buffer));
    return buffer;
  }

  /**
   * Writes a response to the given buffer.
   */
  private ByteBuf writeResponse(ByteBuf buffer, Object request) {
    context.serializer().writeObject(request, OUTPUT.get().setByteBuf(buffer));
    return buffer;
  }

  /**
   * Writes an error to the given buffer.
   */
  private ByteBuf writeError(ByteBuf buffer, Throwable t) {
    context.serializer().writeObject(t, OUTPUT.get().setByteBuf(buffer));
    return buffer;
  }

  /**
   * Reads a request from the given buffer.
   */
  private Object readRequest(ByteBuf buffer) {
    return context.serializer().readObject(INPUT.get().setByteBuf(buffer));
  }

  /**
   * Reads a response from the given buffer.
   */
  private Object readResponse(ByteBuf buffer) {
    return context.serializer().readObject(INPUT.get().setByteBuf(buffer));
  }

  /**
   * Reads an error from the given buffer.
   */
  private Throwable readError(ByteBuf buffer) {
    return context.serializer().readObject(INPUT.get().setByteBuf(buffer));
  }

  /**
   * Handles an exception.
   *
   * @param t The exception to handle.
   */
  void handleException(Throwable t) {
    if (failure == null) {
      failure = t;
      for (CompletableFuture responseFuture : responseFutures.values()) {
        responseFuture.completeExceptionally(t);
      }
      responseFutures.clear();

      for (Listener<Throwable> listener : exceptionListeners) {
        listener.accept(t);
      }
    }
  }

  /**
   * Handles the channel being closed.
   */
  void handleClosed() {
    if (!closed) {
      closed = true;
      for (Listener<Connection> listener : closeListeners) {
        listener.accept(this);
      }
    }
  }

  /**
   * Times out requests.
   */
  void timeout() {
    long time = System.currentTimeMillis();
    Iterator<Map.Entry<Long, ContextualFuture>> iterator = responseFutures.entrySet().iterator();
    while (iterator.hasNext()) {
      ContextualFuture future = iterator.next().getValue();
      if (future.time + REQUEST_TIMEOUT < time) {
        iterator.remove();
        future.context.execute(() -> {
          future.completeExceptionally(new TimeoutException("request timed out"));
        });
      }
    }
  }

  @Override
  public <T, U> CompletableFuture<U> send(T request) {
    Assert.notNull(request, "request");
    Context context = getContext();
    ContextualFuture<U> future = new ContextualFuture<>(System.currentTimeMillis(), context);

    long requestId = ++this.requestId;

    context.executor().execute(() -> {
      ByteBuf buffer = this.channel.alloc().buffer(13);
      buffer.writeByte(REQUEST)
        .writeLong(requestId)
        .writeInt(hashMap.computeIfAbsent(request.getClass(), this::hash32));

      writeFuture = channel.writeAndFlush(writeRequest(buffer, request)).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.context.executor().execute(() -> future.completeExceptionally(new TransportException(channelFuture.cause())));
        }
      });
    });
    return future;
  }

  @Override
  public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
    Assert.notNull(type, "type");
    handlers.put(hashMap.computeIfAbsent(type, this::hash32), new HandlerHolder(handler, getContext()));
    return null;
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    if (failure != null) {
      listener.accept(failure);
    }
    return exceptionListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public Listener<Connection> closeListener(Consumer<Connection> listener) {
    if (closed) {
      listener.accept(this);
    }
    return closeListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    if (writeFuture != null && !writeFuture.isDone()) {
      writeFuture.addListener(channelFuture -> {
        channel.close().addListener(closeFuture -> {
          if (closeFuture.isSuccess()) {
            future.complete(null);
          } else {
            future.completeExceptionally(closeFuture.cause());
          }
        });
      });
    } else {
      channel.close().addListener(closeFuture -> {
        if (closeFuture.isSuccess()) {
          future.complete(null);
        } else {
          future.completeExceptionally(closeFuture.cause());
        }
      });
    }
    return future;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NettyConnection) {
      return ((NettyConnection) object).id().equals(id);
    }
    return false;
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final MessageHandler handler;
    private final Context context;

    private HandlerHolder(MessageHandler handler, Context context) {
      this.handler = handler;
      this.context = context;
    }
  }

  /**
   * Contextual future.
   */
  private static class ContextualFuture<T> extends CompletableFuture<T> {
    private final long time;
    private final Context context;

    private ContextualFuture(long time, Context context) {
      this.time = time;
      this.context = context;
    }
  }

}
