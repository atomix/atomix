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
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import net.kuujo.alleycat.util.ReferenceCounted;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;
import net.openhft.hashing.LongHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyLocalMember.class);
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
  private final LongHashFunction hash = LongHashFunction.city_1_1();
  private final NettyMemberInfo info;
  private Channel channel;
  private ChannelGroup channelGroup;
  private EventLoopGroup workerGroup;
  private boolean listening;
  private CompletableFuture<Member> listenFuture;
  private CompletableFuture<Void> closeFuture;

  NettyLocalMember(NettyMemberInfo info, Type type) {
    super(info, type);
    this.info = info;
  }

  @Override
  public InetSocketAddress address() {
    return info.address();
  }

  /**
   * Hashes the given string to a 32-bit hash.
   */
  private int hash32(String address) {
    long hash = this.hash.hashChars(address);
    return (int)(hash ^ (hash >>> 32));
  }

  @Override
  public <T, U> LocalMember registerHandler(Class<? super T> type, MessageHandler<T, U> handler) {
    return registerHandler(type.getName(), handler);
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    handlers.put(hashMap.computeIfAbsent(topic, this::hash32), new HandlerHolder(handler, getContext()));
    return this;
  }

  @Override
  public LocalMember unregisterHandler(Class<?> type) {
    return unregisterHandler(type.getName());
  }

  @Override
  public LocalMember unregisterHandler(String topic) {
    handlers.remove(hashMap.computeIfAbsent(topic, this::hash32));
    return this;
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
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (!listening)
      return Futures.exceptionalFuture(new IllegalStateException("member not open"));

    ExecutionContext context = getContext();

    CompletableFuture<U> future = new CompletableFuture<>();
    HandlerHolder handler = handlers.get(hashMap.computeIfAbsent(topic, this::hash32));
    if (handler != null) {
      handler.context.execute(() -> {
        handler.handler.handle(message).whenComplete((result, error) -> {
          if (error == null) {
            context.execute(() -> future.complete((U) result));
          } else {
            context.execute(() -> future.completeExceptionally(new ClusterException(error)));
          }
        });
      });
    } else {
      context.execute(() -> future.completeExceptionally(new UnknownTopicException("no handler for the given topic")));
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
  public CompletableFuture<Member> open() {
    if (listening)
      return CompletableFuture.completedFuture(this);

    synchronized (this) {
      if (listenFuture == null) {
        listenFuture = new CompletableFuture<>();
        context.execute(() -> {
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
                pipeline.addLast(new LengthFieldPrepender(2));
                pipeline.addLast(new LengthFieldBasedFrameDecoder(8192, 0, 2, 0, 2));
                pipeline.addLast(new ServerHandlerAdapter());
              }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.ALLOCATOR, ALLOCATOR)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

          // Bind and start to accept incoming connections.
          LOGGER.info("Binding to {}", info.address);
          ChannelFuture bindFuture = bootstrap.bind(info.address.getHostString(), info.address.getPort());
          bindFuture.addListener((ChannelFutureListener) channelFuture -> {
            channelFuture.channel().closeFuture().addListener(closeFuture -> {
              workerGroup.shutdownGracefully();
            });

            if (channelFuture.isSuccess()) {
              channel = channelFuture.channel();
              listening = true;
              info.address = (InetSocketAddress) channel.localAddress();
              context.execute(() -> {
                LOGGER.info("Listening at {}", info.address);
                listenFuture.complete(null);
              });
            } else {
              context.execute(() -> listenFuture.completeExceptionally(channelFuture.cause()));
            }
          });
          channelGroup.add(bindFuture.channel());
        });
      }
    }
    return listenFuture;
  }

  @Override
  public boolean isOpen() {
    return listening;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!listening)
      return CompletableFuture.completedFuture(null);

    synchronized (this) {
      if (closeFuture == null) {
        closeFuture = new CompletableFuture<>();
        context.execute(() -> {
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
        });
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !listening;
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
        NettyLocalMember.this.context.execute(() -> {
          ByteBufBuffer requestBuffer = BUFFER.get();
          requestBuffer.setByteBuf(request);
          Object deserializedRequest = alleycat.readObject(requestBuffer);

          handler.context.execute(() -> {
            handler.handler.handle(deserializedRequest).whenCompleteAsync((result, error) -> {
              if (deserializedRequest instanceof ReferenceCounted) {
                ((ReferenceCounted) deserializedRequest).close();
              }

              if (error == null) {
                ByteBuf response = context.alloc().buffer(10, 1024 * 8);
                response.writeLong(requestId);
                response.writeByte(STATUS_SUCCESS);
                ByteBufBuffer responseBuffer = BUFFER.get();
                responseBuffer.setByteBuf(response);
                alleycat.writeObject(result, responseBuffer);
                context.writeAndFlush(response);
              } else {
                ByteBuf response = context.alloc().buffer(10, 1024 * 8);
                response.writeLong(requestId);
                response.writeByte(STATUS_FAILURE);
                ByteBufBuffer responseBuffer = BUFFER.get();
                responseBuffer.setByteBuf(response);
                alleycat.writeObject(error, responseBuffer);
                context.writeAndFlush(response);
              }

              request.release();
              if (result instanceof ReferenceCounted) {
                ((ReferenceCounted) result).release();
              }
            }, NettyLocalMember.super.context);
          });
        });
      }
    }

    /**
     * Handles a task request.
     */
    private void handleTask(long requestId, ByteBuf request, ChannelHandlerContext context) {
      getContext().execute(() -> {
        ByteBufBuffer requestBuffer = BUFFER.get();
        requestBuffer.setByteBuf(request);
        Task task = alleycat.readObject(requestBuffer);
        try {
          Object result = task.execute();
          ByteBuf response = context.alloc().buffer(9, 1024 * 8);
          ByteBufBuffer responseBuffer = BUFFER.get();
          responseBuffer.setByteBuf(response);
          alleycat.writeObject(result, responseBuffer);
          response.writeLong(requestId);
          context.writeAndFlush(response);
          request.release();
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
