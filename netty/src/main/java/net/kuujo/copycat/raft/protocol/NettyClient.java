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
package net.kuujo.copycat.raft.protocol;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.transport.Client;
import net.kuujo.copycat.transport.Connection;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Netty client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyClient implements Client {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ChannelHandler FIELD_PREPENDER = new LengthFieldPrepender(2);

  private final int id;
  private final EventLoopGroup eventLoopGroup;
  private final Map<Channel, NettyConnection> connections = new ConcurrentHashMap<>();

  public NettyClient(int id, EventLoopGroup eventLoopGroup) {
    this.id = id;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public int id() {
    return id;
  }

  /**
   * Returns the current execution context.
   */
  private Context getContext() {
    Context context = Context.currentContext();
    if (context == null) {
      throw new IllegalStateException("not on a Copycat thread");
    }
    return context;
  }

  @Override
  public CompletableFuture<Connection> connect(InetSocketAddress address) {
    Context context = getContext();
    CompletableFuture<Connection> future = new ComposableFuture<>();

    LOGGER.info("Connecting to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
      .channel(eventLoopGroup instanceof EpollEventLoopGroup ? EpollSocketChannel.class : NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast(FIELD_PREPENDER);
          pipeline.addLast(new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2));
          pipeline.addLast(new ClientHandler(connections, future::complete, context));
        }
      });

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    bootstrap.option(ChannelOption.ALLOCATOR, ALLOCATOR);

    bootstrap.connect(address).addListener(channelFuture -> {
      if (!channelFuture.isSuccess()) {
        future.completeExceptionally(channelFuture.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;
    CompletableFuture[] futures = new CompletableFuture[connections.size()];
    for (Connection connection : connections.values()) {
      futures[i++] = connection.close();
    }
    return CompletableFuture.allOf(futures);
  }

  /**
   * Client handler.
   */
  private class ClientHandler extends NettyHandler {
    private ClientHandler(Map<Channel, NettyConnection> connections, Listener<Connection> listener, Context context) {
      super(connections, listener, context);
    }

    @Override
    public void channelActive(ChannelHandlerContext context) throws Exception {
      Channel channel = context.channel();
      ByteBuf buffer = channel.alloc().buffer(5)
        .writeByte(NettyConnection.CONNECT)
        .writeInt(id);
      channel.writeAndFlush(buffer).addListener(future -> {
        buffer.release();
      });
    }
  }

}
