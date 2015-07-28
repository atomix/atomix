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
package net.kuujo.copycat.raft.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * Netty handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class NettyHandler extends ChannelInboundHandlerAdapter {
  private final Map<Channel, NettyConnection> connections;
  private final Listener<Connection> listener;
  private final Context context;

  protected NettyHandler(Map<Channel, NettyConnection> connections, Listener<Connection> listener, Context context) {
    this.connections = connections;
    this.listener = listener;
    this.context = context;
  }

  /**
   * Adds a connection for the given channel.
   *
   * @param channel The channel for which to add the connection.
   * @param connection The connection to add.
   */
  protected void setConnection(Channel channel, NettyConnection connection) {
    connections.put(channel, connection);
  }

  /**
   * Returns the connection for the given channel.
   *
   * @param channel The channel for which to return the connection.
   * @return The connection.
   */
  protected NettyConnection getConnection(Channel channel) {
    return connections.get(channel);
  }

  /**
   * Removes the connection for the given channel.
   *
   * @param channel The channel for which to remove the connection.
   * @return The connection.
   */
  protected NettyConnection removeConnection(Channel channel) {
    return connections.remove(channel);
  }

  /**
   * Returns the current execution context or creates one.
   */
  private Context getOrCreateContext(Channel channel) {
    Context context = Context.currentContext();
    if (context != null) {
      return context;
    }
    return new SingleThreadContext(Thread.currentThread(), channel.eventLoop(), this.context.serializer().clone());
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, Object message) {
    ByteBuf buffer = (ByteBuf) message;
    int type = buffer.readByte();
    switch (type) {
      case NettyConnection.CONNECT:
        handleConnect(buffer, context);
        break;
      case NettyConnection.OK:
        handleOk(buffer, context);
        break;
      case NettyConnection.REQUEST:
        handleRequest(buffer, context);
        break;
      case NettyConnection.RESPONSE:
        handleResponse(buffer, context);
        break;
    }
  }

  /**
   * Handles a connection identification request.
   */
  private void handleConnect(ByteBuf request, ChannelHandlerContext context) {
    Channel channel = context.channel();
    byte[] idBytes = new byte[request.readInt()];
    request.readBytes(idBytes);
    NettyConnection connection = new NettyConnection(UUID.fromString(new String(idBytes, StandardCharsets.UTF_8)), channel, getOrCreateContext(channel));

    ByteBuf buffer = channel.alloc().buffer(5)
      .writeByte(NettyConnection.OK)
      .writeInt(idBytes.length)
      .writeBytes(idBytes);
    channel.writeAndFlush(buffer).addListener((ChannelFutureListener) channelFuture -> {
      setConnection(channel, connection);
      this.context.execute(() -> listener.accept(connection));
    });
  }

  /**
   * Handles a connection completion request.
   */
  private void handleOk(ByteBuf request, ChannelHandlerContext context) {
    Channel channel = context.channel();
    byte[] idBytes = new byte[request.readInt()];
    request.readBytes(idBytes);
    NettyConnection connection = new NettyConnection(UUID.fromString(new String(idBytes, StandardCharsets.UTF_8)), channel, getOrCreateContext(channel));
    setConnection(channel, connection);
    this.context.execute(() -> listener.accept(connection));
  }

  /**
   * Handles a request.
   */
  private void handleRequest(ByteBuf request, ChannelHandlerContext context) {
    NettyConnection connection = getConnection(context.channel());
    if (connection != null) {
      connection.handleRequest(request);
    }
  }

  /**
   * Handles a response.
   */
  private void handleResponse(ByteBuf response, ChannelHandlerContext context) {
    NettyConnection connection = getConnection(context.channel());
    if (connection != null) {
      connection.handleResponse(response);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, final Throwable t) throws Exception {
    Channel channel = context.channel();
    NettyConnection connection = getConnection(channel);
    if (connection != null) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (Throwable ignore) {
      }
      connection.handleException(t);
    } else {
      channel.close();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    Channel channel = context.channel();
    NettyConnection connection = removeConnection(channel);
    if (connection != null) {
      connection.handleClosed();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      context.close();
    }
  }

}
