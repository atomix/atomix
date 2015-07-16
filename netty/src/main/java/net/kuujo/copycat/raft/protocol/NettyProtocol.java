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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

/**
 * Netty protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyProtocol implements Protocol {
  private final EventLoopGroup eventLoopGroup;
  private final Map<Integer, NettyClient> clients = new ConcurrentHashMap<>();
  private final Map<Integer, NettyServer> servers = new ConcurrentHashMap<>();

  public NettyProtocol() {
    this(Runtime.getRuntime().availableProcessors());
  }

  public NettyProtocol(int threads) {
    if (threads <= 0)
      throw new IllegalArgumentException("threads must be positive");

    ThreadFactory threadFactory = new CopycatThreadFactory("copycat-event-loop-%d");
    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(threads, threadFactory);
    } else {
      eventLoopGroup = new NioEventLoopGroup(threads, threadFactory);
    }
  }

  public NettyProtocol(EventLoopGroup eventLoopGroup) {
    if (eventLoopGroup == null)
      throw new NullPointerException("eventLoopGroup cannot be null");
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public Client client(int id) {
    return clients.computeIfAbsent(id, i -> new NettyClient(id, eventLoopGroup));
  }

  @Override
  public Server server(int id) {
    return servers.computeIfAbsent(id, i -> new NettyServer(id, eventLoopGroup));
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;

    CompletableFuture[] futures = new CompletableFuture[clients.size() + servers.size()];
    for (Client client : clients.values()) {
      futures[i++] = client.close();
    }

    for (Server server : servers.values()) {
      futures[i++] = server.close();
    }

    return CompletableFuture.allOf(futures).thenRun(eventLoopGroup::shutdownGracefully);
  }

}
