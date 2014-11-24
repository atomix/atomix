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
package net.kuujo.copycat.protocol;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.net.URI;
import java.security.cert.CertificateException;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.SSLException;

import net.kuujo.copycat.spi.protocol.ProtocolServer;

/**
 * Netty TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpServer implements ProtocolServer {
  private final NettyTcpProtocol protocol;
  private final URI endpoint;
  private RequestHandler handler;
  private Channel channel;

  public NettyTcpServer(NettyTcpProtocol protocol, URI endpoint) {
    this.protocol = protocol;
    this.endpoint = endpoint;
  }

  @Override
  public void requestHandler(RequestHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    // TODO: Configure proper SSL trust store.
    final SslContext sslContext;
    if (protocol.isSsl()) {
      try {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        sslContext = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
      } catch (SSLException | CertificateException e) {
        future.completeExceptionally(e);
        return future;
      }
    } else {
      sslContext = null;
    }

    final EventLoopGroup serverGroup = new NioEventLoopGroup();
    final EventLoopGroup workerGroup = new NioEventLoopGroup(protocol.getThreads());

    final ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(serverGroup, workerGroup)
    .channel(NioServerSocketChannel.class)
    .childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        if (sslContext != null) {
          pipeline.addLast(sslContext.newHandler(channel.alloc()));
        }
        pipeline.addLast(
            new ObjectEncoder(),
            new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())),
            new TcpProtocolServerHandler(NettyTcpServer.this)
        );
      }
    })
    .option(ChannelOption.SO_BACKLOG, 128);

    if (protocol.getSendBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize());
    }

    if (protocol.getReceiveBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize());
    }

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_REUSEADDR, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.SO_BACKLOG, protocol.getAcceptBacklog());

    if (protocol.getTrafficClass() > -1) {
      bootstrap.option(ChannelOption.IP_TOS, protocol.getTrafficClass());
    }

    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    bootstrap.bind(endpoint.getHost(), endpoint.getPort()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        channelFuture.channel().closeFuture().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            workerGroup.shutdownGracefully();
          }
        });

        if (channelFuture.isSuccess()) {
          channel = channelFuture.channel();
          future.complete(null);
        } else {
          future.completeExceptionally(channelFuture.cause());
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (channel != null) {
      channel.close().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          if (channelFuture.isSuccess()) {
            future.complete(null);
          } else {
            future.completeExceptionally(channelFuture.cause());
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Server request handler.
   */
  private static class TcpProtocolServerHandler extends ChannelInboundHandlerAdapter {
    private final NettyTcpServer server;

    private TcpProtocolServerHandler(NettyTcpServer server) {
      this.server = server;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, Object message) {
      final Request request = (Request) message;
      if (request instanceof PingRequest) {
        context.channel().eventLoop().submit(() -> server.handler.ping((PingRequest) request).thenAccept(context::writeAndFlush));
      } else if (request instanceof SyncRequest) {
        context.channel().eventLoop().submit(() -> server.handler.sync((SyncRequest) request).thenAccept(context::writeAndFlush));
      } else if (request instanceof PollRequest) {
        context.channel().eventLoop().submit(() -> server.handler.poll((PollRequest) request).thenAccept(context::writeAndFlush));
      } else if (request instanceof SubmitRequest) {
        context.channel().eventLoop().submit(() -> server.handler.submit((SubmitRequest) request).thenAccept(context::writeAndFlush));
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      context.close();
    }

  }

}
