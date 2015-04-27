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
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.ExecutionContext;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Netty remote member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyRemoteMember extends AbstractRemoteMember implements NettyMember {
  private static final int RETRY_ATTEMPTS = 3;
  private static final long RECONNECT_INTERVAL = 1000;
  private static final int MESSAGE = 0;
  private static final int TASK = 1;
  private static ThreadLocal<ByteBufBuffer> BUFFER = new ThreadLocal<ByteBufBuffer>() {
    @Override
    protected ByteBufBuffer initialValue() {
      return new ByteBufBuffer();
    }
  };
  final NettyMember.Info info;
  private Serializer serializer;
  private EventLoopGroup eventLoopGroup;
  private boolean eventLoopInitialized;
  private Channel channel;
  private ChannelHandlerContext context;
  private final Map<String, Integer> hashMap = new HashMap<>();
  private final Map<Object, ContextualFuture> responseFutures = new HashMap<>(1024);
  private boolean connected;
  private long requestId;
  private CompletableFuture<RemoteMember> connectFuture;
  private CompletableFuture<Void> closeFuture;
  private ScheduledFuture<?> reconnectFuture;

  NettyRemoteMember(NettyMember.Info info, ExecutionContext context) {
    super(info, context);
    this.info = info;
  }

  /**
   * Sets the remote serializer.
   */
  NettyRemoteMember setSerializer(Serializer serializer) {
    this.serializer = serializer;
    return this;
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
    return info.address;
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
  public CompletableFuture<RemoteMember> connect() {
    if (connected)
      return CompletableFuture.completedFuture(this);

    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          connectFuture = new CompletableFuture<>();
          if (eventLoopGroup == null) {
            eventLoopGroup = new NioEventLoopGroup(1);
          }
          connect(RETRY_ATTEMPTS, RECONNECT_INTERVAL);
        }
      }
    }
    return connectFuture;
  }

  /**
   * Attempts to connect for the given number of attempts.
   */
  private void connect(int attempts, long timeout) {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast(new ClientHandler());
        }
      });

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 15000);

    bootstrap.connect(info.address.getHostString(), info.address.getPort()).addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        channel = channelFuture.channel();
        connected = true;
        connectFuture.complete(this);
      } else if (attempts > 0) {
        reconnectFuture = eventLoopGroup.schedule(() -> connect(attempts - 1, timeout * 2), timeout, TimeUnit.MILLISECONDS);
      } else  {
        connectFuture.completeExceptionally(channelFuture.cause());
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (reconnectFuture != null) {
      reconnectFuture.cancel(false);
      reconnectFuture = null;
    }

    if (!connected)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
          if (channel != null) {
            channel.close().addListener((ChannelFutureListener) channelFuture -> {
              channel = null;
              connected = false;
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
            connected = false;
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
        ByteBufBuffer buffer = BUFFER.get();
        buffer.setByteBuf(response.slice());
        Object result = serializer.readObject(buffer);
        responseFuture.context.execute(() -> {
          responseFuture.complete(result);
        });
      }
      response.release();
    }
  }

}
