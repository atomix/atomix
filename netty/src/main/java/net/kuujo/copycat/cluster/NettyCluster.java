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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.ServiceLoaderResolver;
import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.util.ExecutionContext;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Netty cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyCluster extends ManagedCluster {

  /**
   * Returns a new Netty cluster builder.
   *
   * @return A new Netty cluster builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final EventLoopGroup eventLoopGroup;

  public NettyCluster(EventLoopGroup eventLoopGroup, NettyLocalMember localMember, Collection<NettyRemoteMember> remoteMembers, Alleycat alleycat) {
    super(localMember, remoteMembers, alleycat);
    this.eventLoopGroup = eventLoopGroup;
    remoteMembers.forEach(m -> m.setEventLoopGroup(eventLoopGroup));
  }

  @Override
  protected ManagedRemoteMember createMember(MemberInfo info) {
    ManagedRemoteMember remoteMember = new NettyRemoteMember((NettyMemberInfo) info, Member.Type.PASSIVE)
      .setEventLoopGroup(eventLoopGroup);
    remoteMember.setContext(new ExecutionContext(String.format("copycat-cluster-%d", info.id()), alleycat));
    return remoteMember;
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(eventLoopGroup::shutdownGracefully);
  }

  @Override
  public String toString() {
    return String.format("NettyCluster[member=%d, members=%s]", localMember.id(), members.values().stream().map(Member::id).collect(Collectors.toList()));
  }

  /**
   * Netty cluster builder.
   */
  public static class Builder extends ManagedCluster.Builder<Builder, NettyRemoteMember> {
    private String host;
    private int port;
    private EventLoopGroup eventLoopGroup;

    private Builder() {
    }

    /**
     * Sets the server host.
     *
     * @param host The server host.
     * @return The Netty cluster builder.
     */
    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    /**
     * Sets the server port.
     *
     * @param port The server port.
     * @return The Netty cluster builder.
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * Sets the Netty event loop group.
     *
     * @param eventLoopGroup The Netty event loop group.
     * @return The Netty cluster builder.
     */
    public Builder withEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
      return this;
    }

    @Override
    public ManagedCluster build() {
      NettyMember member = members.remove(memberId);
      NettyLocalMember localMember;
      if (member != null) {
        localMember = new NettyLocalMember(new NettyMemberInfo(memberId, member.address()), Member.Type.ACTIVE);
      } else {
        if (host == null)
          throw new ConfigurationException("member host must be configured");
        localMember = new NettyLocalMember(new NettyMemberInfo(memberId, new InetSocketAddress(host, port)), Member.Type.PASSIVE);
      }

      return new NettyCluster(eventLoopGroup != null ? eventLoopGroup : new NioEventLoopGroup(), localMember, members.values().stream().map(m -> (NettyRemoteMember) m).collect(Collectors.toList()), new Alleycat(new ServiceLoaderResolver()));
    }
  }

}
