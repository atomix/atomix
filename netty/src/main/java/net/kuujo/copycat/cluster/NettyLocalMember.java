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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Netty local member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyLocalMember extends ManagedLocalMember implements NettyMember{
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

  private final Map<Integer, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Map<String, Integer> hashMap = new HashMap<>();
  private final NettyMemberInfo info;
  private Channel channel;
  private ChannelGroup channelGroup;
  private EventLoopGroup workerGroup;
  private boolean listening;
  private CompletableFuture<LocalMember> listenFuture;
  private CompletableFuture<Void> closeFuture;

  NettyLocalMember(NettyMemberInfo info, Type type) {
    super(info, type);
    this.info = info;
  }

  @Override
  public InetSocketAddress address() {
    return info.address();
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())), new HandlerHolder(handler, getContext()));
    return this;
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (!listening)
      return Futures.exceptionalFuture(new IllegalStateException("member not open"));

    CompletableFuture<U> future = new CompletableFuture<>();
    HandlerHolder handler = handlers.get(hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
    if (handler != null) {
      handler.context.execute(() -> {
        handler.handler.handle(message).whenComplete((result, error) -> {
          if (error == null) {
            future.complete((U) result);
          } else {
            future.completeExceptionally(new ClusterException(error));
          }
        });
      });
    } else {
      future.completeExceptionally(new UnknownTopicException("no handler for the given topic"));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    if (!listening)
      return Futures.exceptionalFuture(new IllegalStateException("member not open"));

    CompletableFuture<T> future = new CompletableFuture<>();
    getContext().execute(() -> {
      try {
        future.complete(task.execute());
      } catch (Exception e) {
        future.completeExceptionally(new ClusterException(e));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LocalMember> listen() {
    if (listening)
      return CompletableFuture.completedFuture(this);

    if (listenFuture == null) {
      synchronized (this) {
        if (listenFuture == null) {
          listenFuture = new CompletableFuture<>();

          channelGroup = new DefaultChannelGroup("copycat-acceptor-channels", GlobalEventExecutor.INSTANCE);
          workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

          final ServerBootstrap bootstrap = new ServerBootstrap();
          bootstrap.group(workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.DEBUG))
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2));
                pipeline.addLast(new ServerHandlerAdapter());
              }
            })
            .option(ChannelOption.SO_BACKLOG, 128);

          bootstrap.option(ChannelOption.TCP_NODELAY, true);
          bootstrap.option(ChannelOption.SO_REUSEADDR, true);
          bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
          bootstrap.option(ChannelOption.ALLOCATOR, ALLOCATOR);

          bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

          // Bind and start to accept incoming connections.
          ChannelFuture bindFuture = bootstrap.bind(info.address().getHostString(), info.address().getPort());
          bindFuture.addListener((ChannelFutureListener) channelFuture -> {
            channelFuture.channel().closeFuture().addListener(closeFuture -> {
              workerGroup.shutdownGracefully();
            });

            if (channelFuture.isSuccess()) {
              channel = channelFuture.channel();
              listening = true;
              info.address = (InetSocketAddress) channel.localAddress();
              listenFuture.complete(null);
            } else {
              listenFuture.completeExceptionally(channelFuture.cause());
            }
          });
          channelGroup.add(bindFuture.channel());
        }
      }
    }
    return listenFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!listening)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
          if (channel != null) {
            channel.close().addListener(channelFuture -> {
              listening = false;
              if (channelGroup != null) {
                channelGroup.close();
              }
              workerGroup.shutdownGracefully();
              if (channelFuture.isSuccess()) {
                closeFuture.complete(null);
              } else {
                closeFuture.completeExceptionally(channelFuture.cause());
              }
            });
          } else {
            if (channelGroup != null) {
              channelGroup.close();
            }
            workerGroup.shutdownGracefully();
            closeFuture.complete(null);
          }
        }
      }
    }
    return closeFuture;
  }

  /**
   * Server request handler.
   */
  private class ServerHandlerAdapter extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(final ChannelHandlerContext context, Object message) {
      ByteBuf request = (ByteBuf) message;
      long requestId = request.readLong();
      int type = request.readByte();
      if (type == MESSAGE) {
        handleMessage(requestId, request, context);
      } else if (type == TASK) {
        handleTask(requestId, request, context);
      }
    }

    /**
     * Handles a message request.
     */
    private void handleMessage(long requestId, ByteBuf request, ChannelHandlerContext context) {
      int address = request.readInt();
      HandlerHolder handler = handlers.get(address);
      if (handler != null) {
        ByteBufBuffer requestBuffer = BUFFER.get();
        requestBuffer.setByteBuf(request);
        Object deserializedRequest = serializer.readObject(requestBuffer);
        handler.context.execute(() -> {
          handler.handler.handle(deserializedRequest).whenComplete((result, error) -> {
            if (error == null) {
              context.channel().eventLoop().execute(() -> {
                ByteBuf response = context.alloc().buffer(10, 1024 * 8);
                response.writeLong(requestId);
                response.writeByte(STATUS_SUCCESS);
                ByteBufBuffer responseBuffer = BUFFER.get();
                responseBuffer.setByteBuf(response);
                serializer.writeObject(result, responseBuffer);
                context.writeAndFlush(response);
                request.release();
              });
            } else {
              context.channel().eventLoop().execute(() -> {
                ByteBuf response = context.alloc().buffer(10, 1024 * 8);
                response.writeLong(requestId);
                response.writeByte(STATUS_FAILURE);
                ByteBufBuffer responseBuffer = BUFFER.get();
                responseBuffer.setByteBuf(response);
                serializer.writeObject(error, responseBuffer);
                context.writeAndFlush(response);
                request.release();
              });
            }
          });
        });
      }
    }

    /**
     * Handles a task request.
     */
    private void handleTask(long requestId, ByteBuf request, ChannelHandlerContext context) {
      ByteBufBuffer requestBuffer = BUFFER.get();
      requestBuffer.setByteBuf(request);
      Task task = serializer.readObject(requestBuffer);
      getContext().execute(() -> {
        try {
          Object result = task.execute();
          context.channel().eventLoop().execute(() -> {
            ByteBuf response = context.alloc().buffer(9, 1024 * 8);
            response.writeLong(requestId);
            ByteBufBuffer responseBuffer = BUFFER.get();
            responseBuffer.setByteBuf(response);
            serializer.writeObject(result, responseBuffer);
            context.writeAndFlush(response);
            request.release();
          });
        } catch (Exception e) {

        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      context.close();
    }
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final MessageHandler<Object, Object> handler;
    private final ExecutionContext context;

    private HandlerHolder(MessageHandler handler, ExecutionContext context) {
      this.handler = handler;
      this.context = context;
    }
  }

}
