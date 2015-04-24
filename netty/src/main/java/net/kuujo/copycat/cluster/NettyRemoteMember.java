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
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Netty remote member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyRemoteMember extends AbstractRemoteMember {

  /**
   * Returns a new Netty remote member builder.
   *
   * @return A new Netty remote member builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final int MESSAGE = 0;
  private static final int TASK = 1;
  private static ThreadLocal<ByteBufBuffer> BUFFER = new ThreadLocal<ByteBufBuffer>() {
    @Override
    protected ByteBufBuffer initialValue() {
      return new ByteBufBuffer();
    }
  };
  private final String host;
  private final int port;
  private Channel channel;
  private ChannelHandlerContext context;
  private final Map<String, Integer> hashMap = new HashMap<>();
  private final Map<Object, CompletableFuture> responseFutures = new HashMap<>(1024);
  private boolean connected;
  private long requestId;
  private CompletableFuture<RemoteMember> connectFuture;
  private CompletableFuture<Void> closeFuture;

  protected NettyRemoteMember(String host, int port, Info info, CopycatSerializer serializer, ExecutionContext context) {
    super(info, serializer, context);
    this.host = host;
    this.port = port;
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    final CompletableFuture<U> future = new CompletableFuture<>();
    if (channel != null) {
      long requestId = ++this.requestId;
      ByteBufBuffer buffer = BUFFER.get();
      ByteBuf byteBuf = context.alloc().buffer(8, 1024 * 8);
      byteBuf.writerIndex(13);
      buffer.setByteBuf(byteBuf);
      serializer.writeObject(message, buffer);
      byteBuf.setByte(0, MESSAGE);
      byteBuf.setLong(1, requestId);
      byteBuf.setInt(9, hashMap.computeIfAbsent(topic, t -> HashFunctions.CITYHASH.hash32(t.getBytes())));
      channel.writeAndFlush(buffer).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.completeExceptionally(new ClusterException(channelFuture.cause()));
        }
      });
    } else {
      future.completeExceptionally(new ClusterException("Client not connected"));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return submit(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    if (channel != null) {
      long requestId = ++this.requestId;
      ByteBufBuffer buffer = BUFFER.get();
      ByteBuf byteBuf = context.alloc().buffer(8, 1024 * 8);
      byteBuf.writerIndex(9);
      buffer.setByteBuf(byteBuf);
      serializer.writeObject(task, buffer);
      byteBuf.setByte(0, TASK);
      byteBuf.setLong(1, requestId);
      channel.writeAndFlush(buffer).addListener((channelFuture) -> {
        if (channelFuture.isSuccess()) {
          responseFutures.put(requestId, future);
        } else {
          future.completeExceptionally(new ClusterException(channelFuture.cause()));
        }
      });
    } else {
      future.completeExceptionally(new ClusterException("Client not connected"));
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
          final EventLoopGroup group = new NioEventLoopGroup(1);
          Bootstrap bootstrap = new Bootstrap();
          bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(channelHandler);
              }
            });

          bootstrap.option(ChannelOption.TCP_NODELAY, true);
          bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
          bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 15000);

          bootstrap.connect(host, port).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
              channel = channelFuture.channel();
              connected = true;
              connectFuture.complete(this);
            } else  {
              connectFuture.completeExceptionally(channelFuture.cause());
            }
          });
        }
      }
    }
    return connectFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!connected)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
          if (channel != null) {
            channel.close().addListener(new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                channel = null;
                connected = false;
                if (channelFuture.isSuccess()) {
                  closeFuture.complete(null);
                } else {
                  closeFuture.completeExceptionally(channelFuture.cause());
                }
              }
            });
          } else {
            connected = false;
            closeFuture.complete(null);
          }
        }
      }
    }
    return closeFuture;
  }

  @SuppressWarnings("unchecked")
  private final ChannelInboundHandlerAdapter channelHandler = new ChannelInboundHandlerAdapter() {
    @Override
    public void channelActive(ChannelHandlerContext context) {
      NettyRemoteMember.this.context = context;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
      ByteBuf response = (ByteBuf) message;
      long responseId = response.readLong();
      CompletableFuture responseFuture = responseFutures.remove(responseId);
      if (responseFuture != null) {
        ByteBufBuffer buffer = BUFFER.get();
        buffer.setByteBuf(response.slice());
        Object result = serializer.readObject(buffer);
        responseFuture.complete(result);
      }
      response.release();
    }
  };

  /**
   * Netty remote member builder.
   */
  public static class Builder extends AbstractRemoteMember.Builder<Builder, NettyRemoteMember> {
    private String host;
    private int port;

    /**
     * Sets the member host.
     *
     * @param host The member host.
     * @return The member builder.
     */
    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    /**
     * Sets the member port.
     *
     * @param port The member port.
     * @return The member builder.
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    @Override
    public NettyRemoteMember build() {
      return new NettyRemoteMember(host != null ? host : "localhost", port, new Info(id, type), serializer, new ExecutionContext(String.format("copycat-cluster-%d", id)));
    }
  }

}
