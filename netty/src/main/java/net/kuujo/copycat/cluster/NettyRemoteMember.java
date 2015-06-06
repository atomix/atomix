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
package net.kuujo.copycat.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty remote member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyRemoteMember extends ManagedRemoteMember implements NettyMember {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyRemoteMember.class);
  private static final long MAX_RECONNECT_INTERVAL = 60000;
  private static final long INITIAL_RECONNECT_INTERVAL = 100;
  private static final int MESSAGE = 0;
  private static final int TASK = 1;
  private static final int STATUS_FAILURE = 0;
  private static final int STATUS_SUCCESS = 1;
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ThreadLocal<ByteBufBuffer> BUFFER = new ThreadLocal<ByteBufBuffer>() {
    @Override
    protected ByteBufBuffer initialValue() {
      return new ByteBufBuffer();
    }
  };

  private final NettyMemberInfo info;
  private EventLoopGroup eventLoopGroup;
  private boolean eventLoopInitialized;
  private Channel channel;
  private ChannelHandlerContext context;
  private final Map<String, Integer> hashMap = new HashMap<>();
  private final Map<Object, ContextualFuture> responseFutures = new HashMap<>(1024);
  private final AtomicBoolean connecting = new AtomicBoolean();
  private final AtomicBoolean connected = new AtomicBoolean();
  private long requestId;
  private CompletableFuture<Void> closeFuture;
  private ScheduledFuture<?> connectFuture;

  NettyRemoteMember(NettyMemberInfo info, Type type) {
    super(info, type);
    this.info = info;
  }

  @Override
  public NettyMemberInfo info() {
    return info;
  }

  /**
   * Sets the Netty event loop group.
   */
  NettyRemoteMember setEventLoopGroup(EventLoopGroup eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
    eventLoopInitialized = true;
    return this;
  }

  @Override
  public InetSocketAddress address() {
    return info.address();
  }

  @Override
  public <T, U> CompletableFuture<U> send(T message) {
    return send(message.getClass().getName(), message);
  }

  @Override
  public <T, U> CompletableFuture<U> send(Class<? super T> type, T message) {
    return send(type.getName(), message);
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    final ContextualFuture<U> future = new ContextualFuture<>(getContext());
    if (channel != null) {
      long requestId = ++this.requestId;
      ByteBufBuffer buffer = BUFFER.get();
      ByteBuf byteBuf = context.alloc().buffer(13, 1024 * 8);
      byteBuf.writerIndex(13);
      buffer.setByteBuf(byteBuf);
      serializer.writeObject(message, buffer);
      byteBuf.setLong(0, requestId);
      byteBuf.setByte(8, MESSAGE);
      byteBuf.setInt(9, hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
      channel.writeAndFlush(byteBuf).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.context.execute(() -> {
            future.completeExceptionally(new ClusterException(channelFuture.cause()));
          });
        }
      });
    } else {
      future.context.execute(() -> {
        future.completeExceptionally(new ClusterException("Client not connected"));
      });
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    final ContextualFuture<T> future = new ContextualFuture<>(getContext());
    if (channel != null) {
      long requestId = ++this.requestId;
      ByteBufBuffer buffer = BUFFER.get();
      ByteBuf byteBuf = context.alloc().buffer(9, 1024 * 8);
      byteBuf.writerIndex(9);
      buffer.setByteBuf(byteBuf);
      serializer.writeObject(task, buffer);
      byteBuf.setLong(0, requestId);
      byteBuf.setByte(8, TASK);
      channel.writeAndFlush(byteBuf).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.context.execute(() -> {
            future.completeExceptionally(new ClusterException(channelFuture.cause()));
          });
        }
      });
    } else {
      future.context.execute(() -> {
        future.completeExceptionally(new ClusterException("Client not connected"));
      });
    }
    return future;
  }

  @Override
  public synchronized CompletableFuture<RemoteMember> connect() {
    if (connecting.compareAndSet(false, true))
      connect(INITIAL_RECONNECT_INTERVAL);
    return CompletableFuture.completedFuture(this);
  }

  /**
   * Attempts to connect to the server.
   */
  private synchronized void connect(long timeout) {
    LOGGER.info("Connecting to {}...", info.address);
    doConnect(timeout, () -> connect(Math.min(timeout * 2, MAX_RECONNECT_INTERVAL)));
  }

  /**
   * Attempts to reconnect to the server.
   */
  private void reconnect(long timeout) {
    LOGGER.info("Reconnecting to {}...", info.address);
    doConnect(timeout, () -> reconnect(Math.min(timeout * 2, MAX_RECONNECT_INTERVAL)));
  }

  /**
   * Attempts to connect to the server.
   */
  private void doConnect(long timeout, Runnable reschedule) {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast(new LengthFieldPrepender(2));
          pipeline.addLast(new ClientHandler());
        }
      });

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    bootstrap.option(ChannelOption.ALLOCATOR, ALLOCATOR);

    bootstrap.connect(info.address().getHostString(), info.address().getPort()).addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        LOGGER.info("Connected to {}", info.address);
        channel = channelFuture.channel();
        connecting.set(false);
        connected.set(true);
      } else {
        connectFuture = eventLoopGroup.schedule(reschedule, timeout, TimeUnit.MILLISECONDS);
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (connectFuture != null) {
      connectFuture.cancel(false);
      connectFuture = null;
    }

    connecting.set(false);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
          if (channel != null) {
            LOGGER.info("Disconnecting from {}", info.address);
            channel.close().addListener((ChannelFutureListener) channelFuture -> {
              channel = null;
              connected.set(false);
              if (!eventLoopInitialized && eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully();
                eventLoopGroup = null;
              }
              if (channelFuture.isSuccess()) {
                closeFuture.complete(null);
              } else {
                closeFuture.completeExceptionally(channelFuture.cause());
              }
            });
          } else {
            connected.set(false);
            if (!eventLoopInitialized && eventLoopGroup != null) {
              eventLoopGroup.shutdownGracefully();
              eventLoopGroup = null;
            }
            closeFuture.complete(null);
          }
        }
      }
    }
    return closeFuture;
  }

  /**
   * Contextual future.
   */
  private static class ContextualFuture<T> extends CompletableFuture<T> {
    private final ExecutionContext context;

    private ContextualFuture(ExecutionContext context) {
      this.context = context;
    }
  }

  /**
   * Client channel handler.
   */
  private class ClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext context) {
      NettyRemoteMember.this.context = context;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void channelRead(ChannelHandlerContext context, Object message) {
      ByteBuf response = (ByteBuf) message;
      long responseId = response.readLong();
      ContextualFuture responseFuture = responseFutures.remove(responseId);
      if (responseFuture != null) {
        int status = response.readByte();
        ByteBufBuffer buffer = BUFFER.get();
        buffer.setByteBuf(response.slice());
        Object result = serializer.readObject(buffer);
        responseFuture.context.execute(() -> {
          if (status == STATUS_FAILURE) {
            responseFuture.completeExceptionally((Exception) result);
          } else {
            responseFuture.complete(result);
          }
        });
      }
      response.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
      LOGGER.info("Lost connection to {}: {}", info.address, cause != null ? cause.getMessage() : "unknown");
      context.close();
      connected.set(false);
      channel = null;
      NettyRemoteMember.this.context = null;
      if (connecting.compareAndSet(false, true)) {
        reconnect(INITIAL_RECONNECT_INTERVAL);
      }
    }
  }

}
