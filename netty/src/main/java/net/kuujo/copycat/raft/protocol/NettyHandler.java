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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Netty handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class NettyHandler extends ChannelInboundHandlerAdapter {

  /**
   * Returns the connection for the given channel.
   *
   * @param channel The channel for which to return the connection.
   * @return The connection.
   */
  protected abstract NettyConnection getConnection(Channel channel);

  /**
   * Removes the connection for the given channel.
   *
   * @param channel The channel for which to remove the connection.
   * @return The connection.
   */
  protected abstract NettyConnection removeConnection(Channel channel);

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
  public void channelInactive(ChannelHandlerContext chctx) throws Exception {
    Channel ch = chctx.channel();
    NettyConnection connection = removeConnection(ch);
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
