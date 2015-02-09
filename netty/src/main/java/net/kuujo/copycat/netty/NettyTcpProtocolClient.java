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
package net.kuujo.copycat.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;

import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Netty TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpProtocolClient implements ProtocolClient {
  private final String host;
  private final int port;
  private final NettyTcpProtocol protocol;
  private EventLoopGroup group;
  private Channel channel;
  private ChannelHandlerContext context;
  private final Map<Object, CompletableFuture<ByteBuffer>> responseFutures = new HashMap<>(1000);
  private long requestId;

  private final ChannelInboundHandlerAdapter channelHandler = new SimpleChannelInboundHandler<byte[]>() {
    @Override
    public void channelActive(ChannelHandlerContext context) {
      NettyTcpProtocolClient.this.context = context;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, byte[] message) throws Exception {
      ByteBuffer buffer = ByteBuffer.wrap(message);
      long responseId = buffer.getLong();
      CompletableFuture<ByteBuffer> responseFuture = responseFutures.remove(responseId);
      if (responseFuture != null) {
        responseFuture.complete(buffer.slice());
      }
    }
  };


  public NettyTcpProtocolClient(String host, int port, NettyTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    if (channel != null) {
      long requestId = ++this.requestId;
      ByteBuffer buffer = ByteBuffer.allocate(request.limit() + 8);
      buffer.putLong(requestId);
      buffer.put(request);
      channel.writeAndFlush(buffer.array()).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.completeExceptionally(new ProtocolException(channelFuture.cause()));
        }
      });
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> connect() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (channel != null) {
      future.complete(null);
      return future;
    }

    final SslContext sslContext;
    if (protocol.isSsl()) {
      try {
        sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
      } catch (SSLException e) {
        future.completeExceptionally(e);
        return future;
      }
    } else {
      sslContext = null;
    }

    group = new NioEventLoopGroup(protocol.getThreads());
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(group)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(channel.alloc(), host, port));
          }
          pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
          pipeline.addLast("bytesDecoder", new ByteArrayDecoder());
          pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
          pipeline.addLast("bytesEncoder", new ByteArrayEncoder());
          pipeline.addLast("handler", channelHandler);
        }
      });

    if (protocol.getSendBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize());
    }

    if (protocol.getReceiveBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize());
    }

    if (protocol.getTrafficClass() > -1) {
      bootstrap.option(ChannelOption.IP_TOS, protocol.getTrafficClass());
    }

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_LINGER, protocol.getSoLinger());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, protocol.getConnectTimeout());

    bootstrap.connect(host, port).addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        channel = channelFuture.channel();
        future.complete(null);
      } else  {
        future.completeExceptionally(channelFuture.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (channel != null) {
      channel.close().addListener(channelFuture -> {
        group.shutdownGracefully();
        if (channelFuture.isSuccess()) {
          future.complete(null);
        } else {
          future.completeExceptionally(channelFuture.cause());
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
