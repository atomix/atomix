package net.kuujo.copycat.netty.protocol.impl;

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

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.AsyncResult;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.Request;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

public class TcpProtocolServer implements ProtocolServer {
  private final TcpProtocol protocol;
  private ProtocolHandler handler;
  private Channel channel;

  public TcpProtocolServer(TcpProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public void start(final AsyncCallback<Void> callback) {
    // TODO: Configure proper SSL trust store.
    final SslContext sslContext;
    if (protocol.isUseSsl()) {
      try {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        sslContext = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
      } catch (SSLException | CertificateException e) {
        if (callback != null) {
          callback.call(new AsyncResult<Void>(e));
        }
        return;
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
            new TcpProtocolServerHandler(TcpProtocolServer.this)
        );
      }
    })
    .option(ChannelOption.SO_BACKLOG, 128)
    .option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize())
    .option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize())
    .childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    bootstrap.bind(protocol.getHost(), protocol.getPort()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        future.channel().closeFuture().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            workerGroup.shutdownGracefully();
          }
        });

        if (future.isSuccess()) {
          channel = future.channel();
          if (callback != null) {
            callback.call(new AsyncResult<Void>((Void) null));
          }
        } else if (future.cause() != null && callback != null) {
          callback.call(new AsyncResult<Void>(future.cause()));
        }
      }
    });
  }

  @Override
  public void stop(final AsyncCallback<Void> callback) {
    if (channel != null) {
      channel.close().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            if (callback != null) {
              callback.call(new AsyncResult<Void>((Void) null));
            }
          } else if (future.cause() != null && callback != null) {
            callback.call(new AsyncResult<Void>(future.cause()));
          }
        }
      });
    } else if (callback != null) {
      callback.call(new AsyncResult<Void>((Void) null));
    }
  }

  /**
   * Server request handler.
   */
  private static class TcpProtocolServerHandler extends ChannelInboundHandlerAdapter {
    private final TcpProtocolServer server;

    private TcpProtocolServerHandler(TcpProtocolServer server) {
      this.server = server;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, Object message) {
      final Request request = (Request) message;
      if (request instanceof AppendEntriesRequest) {
        context.channel().eventLoop().submit(new Runnable() {
          @Override
          public void run() {
            server.handler.appendEntries((AppendEntriesRequest) request, new AsyncCallback<AppendEntriesResponse>() {
              @Override
              public void call(AsyncResult<AppendEntriesResponse> result) {
                if (result.succeeded()) {
                  context.writeAndFlush(result.value());
                }
              }
            });
          }
        });
      } else if (request instanceof RequestVoteRequest) {
        context.channel().eventLoop().submit(new Runnable() {
          @Override
          public void run() {
            server.handler.requestVote((RequestVoteRequest) request, new AsyncCallback<RequestVoteResponse>() {
              @Override
              public void call(AsyncResult<RequestVoteResponse> result) {
                if (result.succeeded()) {
                  context.writeAndFlush(result.value());
                }
              }
            });
          }
        });
      } else if (request instanceof SubmitCommandRequest) {
        context.channel().eventLoop().submit(new Runnable() {
          @Override
          public void run() {
            server.handler.submitCommand((SubmitCommandRequest) request, new AsyncCallback<SubmitCommandResponse>() {
              @Override
              public void call(AsyncResult<SubmitCommandResponse> result) {
                if (result.succeeded()) {
                  context.writeAndFlush(result.value());
                }
              }
            });
          }
        });
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      context.close();
    }

  }

}
