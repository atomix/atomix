package net.kuujo.copycat.netty.protocol.impl;

import io.netty.bootstrap.Bootstrap;
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
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLException;

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.AsyncResult;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

public class TcpProtocolClient implements ProtocolClient {
  private final TcpProtocol protocol;
  private Channel channel;
  private final Map<Object, AsyncCallback<? extends Response>> responseHandlers = new HashMap<>();

  public TcpProtocolClient(TcpProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public void appendEntries(final AppendEntriesRequest request, final AsyncCallback<AppendEntriesResponse> callback) {
    if (channel != null) {
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            responseHandlers.put(request.id(), callback);
          } else {
            callback.call(new AsyncResult<AppendEntriesResponse>(new ProtocolException(future.cause())));
          }
        }
      });
    } else {
      callback.call(new AsyncResult<AppendEntriesResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void requestVote(final RequestVoteRequest request, final AsyncCallback<RequestVoteResponse> callback) {
    if (channel != null) {
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            responseHandlers.put(request.id(), callback);
          } else {
            callback.call(new AsyncResult<RequestVoteResponse>(new ProtocolException(future.cause())));
          }
        }
      });
    } else {
      callback.call(new AsyncResult<RequestVoteResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void submitCommand(final SubmitCommandRequest request, final AsyncCallback<SubmitCommandResponse> callback) {
    if (channel != null) {
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            responseHandlers.put(request.id(), callback);
          } else {
            callback.call(new AsyncResult<SubmitCommandResponse>(new ProtocolException(future.cause())));
          }
        }
      });
    } else {
      callback.call(new AsyncResult<SubmitCommandResponse>(new ProtocolException("Client not connected")));
    }
  }

  @Override
  public void connect() {
    connect(null);
  }

  @Override
  public void connect(final AsyncCallback<Void> callback) {
    if (channel != null) {
      if (callback != null) {
        callback.call(new AsyncResult<Void>((Void) null));
      }
      return;
    }

    final SslContext sslContext;
    if (protocol.isUseSsl()) {
      try {
        sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
      } catch (SSLException e) {
        if (callback != null) {
          callback.call(new AsyncResult<Void>(e));
        }
        return;
      }
    } else {
      sslContext = null;
    }

    final EventLoopGroup group = new NioEventLoopGroup(protocol.getThreads());
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(group)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(channel.alloc(), protocol.getHost(), protocol.getPort()));
          }
          pipeline.addLast(
              new ObjectEncoder(),
              new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())),
              new TcpProtocolClientHandler(TcpProtocolClient.this)
          );
        }
      })
      .option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize())
      .option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize());

    bootstrap.connect(protocol.getHost(), protocol.getPort()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
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
  public void close() {
    close(null);
  }

  @Override
  public void close(final AsyncCallback<Void> callback) {
    if (channel != null) {
      channel.close().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          channel = null;
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
   * Client response handler.
   */
  private static class TcpProtocolClientHandler extends ChannelInboundHandlerAdapter {
    private final TcpProtocolClient client;

    private TcpProtocolClientHandler(TcpProtocolClient client) {
      this.client = client;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void channelRead(final ChannelHandlerContext context, Object message) {
      Response response = (Response) message;
      AsyncCallback<? extends Response> responseHandler = client.responseHandlers.remove(response.id());
      if (responseHandler != null) {
        responseHandler.call(new AsyncResult(response));
      }
    }
  }

}
