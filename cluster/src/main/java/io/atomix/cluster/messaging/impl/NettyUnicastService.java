/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.impl.AddressSerializer;
import io.atomix.cluster.messaging.ManagedUnicastService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Netty unicast service.
 */
public class NettyUnicastService implements ManagedUnicastService {

  /**
   * Returns a new unicast service builder.
   *
   * @return a new unicast service builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Netty unicast service builder.
   */
  public static class Builder implements UnicastService.Builder {
    private Address address;

    /**
     * Sets the local address.
     *
     * @param address the local address
     * @return the unicast service builder
     */
    public Builder withAddress(Address address) {
      this.address = checkNotNull(address);
      return this;
    }

    @Override
    public ManagedUnicastService build() {
      return new NettyUnicastService(address);
    }
  }

  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(Message.class)
      .register(new AddressSerializer(), Address.class)
      .build());

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final Address address;
  private EventLoopGroup group;
  private DatagramChannel channel;

  private final Map<String, Map<BiConsumer<Address, byte[]>, Executor>> listeners = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public NettyUnicastService(Address address) {
    this.address = address;
  }

  @Override
  public void unicast(Address address, String subject, byte[] payload) {
    Message message = new Message(this.address, subject, payload);
    byte[] bytes = SERIALIZER.encode(message);
    ByteBuf buf = channel.alloc().buffer(4 + bytes.length);
    buf.writeInt(bytes.length).writeBytes(bytes);
    channel.writeAndFlush(new DatagramPacket(buf, new InetSocketAddress(address.address(), address.port())));
  }

  @Override
  public synchronized void addListener(String subject, BiConsumer<Address, byte[]> listener, Executor executor) {
    listeners.computeIfAbsent(subject, s -> Maps.newConcurrentMap()).put(listener, executor);
  }

  @Override
  public synchronized void removeListener(String subject, BiConsumer<Address, byte[]> listener) {
    Map<BiConsumer<Address, byte[]>, Executor> listeners = this.listeners.get(subject);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        this.listeners.remove(subject);
      }
    }
  }

  private CompletableFuture<Void> bootstrap() {
    Bootstrap serverBootstrap = new Bootstrap()
        .group(group)
        .channel(NioDatagramChannel.class)
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext context, DatagramPacket packet) throws Exception {
            byte[] payload = new byte[packet.content().readInt()];
            packet.content().readBytes(payload);
            Message message = SERIALIZER.decode(payload);
            Map<BiConsumer<Address, byte[]>, Executor> listeners = NettyUnicastService.this.listeners.get(message.subject());
            if (listeners != null) {
              listeners.forEach((consumer, executor) ->
                  executor.execute(() -> consumer.accept(message.source(), message.payload())));
            }
          }
        })
        .option(ChannelOption.SO_BROADCAST, true)
        .option(ChannelOption.SO_REUSEADDR, true);

    CompletableFuture<Void> future = new CompletableFuture<>();
    serverBootstrap.bind(new InetSocketAddress(address.address(), address.port())).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        channel = (DatagramChannel) f.channel();
        future.complete(null);
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<UnicastService> start() {
    group = new NioEventLoopGroup(0, namedThreads("netty-unicast-event-nio-client-%d", log));
    return bootstrap()
        .thenRun(() -> started.set(true))
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (channel != null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      channel.close().addListener(f -> {
        started.set(false);
        group.shutdownGracefully();
        future.complete(null);
      });
      return future;
    }
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Internal unicast service message.
   */
  static class Message {
    private final Address source;
    private final String subject;
    private final byte[] payload;

    Message() {
      this(null, null, null);
    }

    Message(Address source, String subject, byte[] payload) {
      this.source = source;
      this.subject = subject;
      this.payload = payload;
    }

    Address source() {
      return source;
    }

    String subject() {
      return subject;
    }

    byte[] payload() {
      return payload;
    }
  }
}
