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
package io.atomix.messaging.impl;

import com.google.common.collect.Sets;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.ManagedBroadcastService;
import io.atomix.utils.AtomixRuntimeException;
import io.atomix.utils.net.Address;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Netty broadcast service.
 */
public class NettyBroadcastService implements ManagedBroadcastService {

  /**
   * Returns a new broadcast service builder.
   *
   * @return a new broadcast service builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Netty broadcast service builder.
   */
  public static class Builder implements BroadcastService.Builder {
    private Address localAddress;
    private Address groupAddress;
    private boolean enabled = true;

    /**
     * Sets the local address.
     *
     * @param address the local address
     * @return the broadcast service builder
     */
    public Builder withLocalAddress(Address address) {
      this.localAddress = checkNotNull(address);
      return this;
    }

    /**
     * Sets the group address.
     *
     * @param address the group address
     * @return the broadcast service builder
     */
    public Builder withGroupAddress(Address address) {
      this.groupAddress = checkNotNull(address);
      return this;
    }

    /**
     * Sets whether the service is enabled.
     *
     * @param enabled whether the service is enabled
     * @return the broadcast service builder
     */
    public Builder withEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    @Override
    public ManagedBroadcastService build() {
      return new NettyBroadcastService(localAddress, groupAddress, enabled);
    }
  }

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final boolean enabled;
  private final InetSocketAddress localAddress;
  private final InetSocketAddress groupAddress;
  private final NetworkInterface iface;
  private EventLoopGroup group;
  private Channel serverChannel;
  private DatagramChannel clientChannel;

  private final Set<Consumer<byte[]>> listeners = Sets.newCopyOnWriteArraySet();
  private final AtomicBoolean started = new AtomicBoolean();

  public NettyBroadcastService(Address localAddress, Address groupAddress, boolean enabled) {
    this.enabled = enabled;
    this.localAddress = new InetSocketAddress(localAddress.host(), groupAddress.port());
    this.groupAddress = new InetSocketAddress(groupAddress.host(), groupAddress.port());
    try {
      iface = NetworkInterface.getByInetAddress(localAddress.address());
    } catch (SocketException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  @Override
  public void broadcast(byte[] message) {
    if (enabled) {
      ByteBuf buf = serverChannel.alloc().buffer();
      buf.writeInt(message.length).writeBytes(message);
      serverChannel.writeAndFlush(new DatagramPacket(buf, groupAddress));
    }
  }

  @Override
  public void addListener(Consumer<byte[]> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<byte[]> listener) {
    listeners.remove(listener);
  }

  private CompletableFuture<Void> bootstrapServer() {
    Bootstrap serverBootstrap = new Bootstrap()
        .group(group)
        .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
        .handler(new SimpleChannelInboundHandler<Object>() {
          @Override
          public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            // Nothing will be sent.
          }
        })
        .option(ChannelOption.IP_MULTICAST_IF, iface)
        .option(ChannelOption.SO_REUSEADDR, true);

    CompletableFuture<Void> future = new CompletableFuture<>();
    serverBootstrap.bind(localAddress).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        serverChannel = f.channel();
        future.complete(null);
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    return future;
  }

  private CompletableFuture<Void> bootstrapClient() {
    Bootstrap clientBootstrap = new Bootstrap()
        .group(group)
        .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext context, DatagramPacket packet) throws Exception {
            byte[] message = new byte[packet.content().readInt()];
            packet.content().readBytes(message);
            for (Consumer<byte[]> listener : listeners) {
              listener.accept(message);
            }
          }
        })
        .option(ChannelOption.IP_MULTICAST_IF, iface)
        .option(ChannelOption.SO_REUSEADDR, true)
        .localAddress(localAddress.getPort());

    CompletableFuture<Void> future = new CompletableFuture<>();
    clientBootstrap.bind().addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        clientChannel = (DatagramChannel) f.channel();
        log.info("{} joining multicast group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
        clientChannel.joinGroup(groupAddress, iface).addListener(f2 -> {
          if (f2.isSuccess()) {
            log.info("{} successfully joined multicast group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
            future.complete(null);
          } else {
            log.info("{} failed to join group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
            future.completeExceptionally(f2.cause());
          }
        });
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<BroadcastService> start() {
    if (!enabled) {
      return CompletableFuture.completedFuture(this);
    }
    group = new NioEventLoopGroup(0, namedThreads("netty-broadcast-event-nio-client-%d", log));
    return bootstrapServer()
        .thenCompose(v -> bootstrapClient())
        .thenRun(() -> started.set(true))
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (!enabled) {
      return CompletableFuture.completedFuture(null);
    }
    if (clientChannel != null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clientChannel.leaveGroup(groupAddress, iface).addListener(f -> {
        started.set(false);
        future.complete(null);
      });
      return future;
    }
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
