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
import net.kuujo.copycat.util.ExecutionContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Netty cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyCluster extends AbstractCluster {

  /**
   * Returns a new Netty cluster builder.
   *
   * @return A new Netty cluster builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final EventLoopGroup eventLoopGroup;

  public NettyCluster(EventLoopGroup eventLoopGroup, NettyLocalMember localMember, Collection<NettyRemoteMember> remoteMembers) {
    super(localMember, remoteMembers);
    this.eventLoopGroup = eventLoopGroup;
    remoteMembers.forEach(m -> m.setEventLoopGroup(eventLoopGroup));
  }

  @Override
  protected AbstractRemoteMember createRemoteMember(AbstractMember.Info info) {
    return new NettyRemoteMember((NettyMember.Info) info, localMember.serializer.copy(), new ExecutionContext(String.format("copycat-cluster-%d", info.id()))).setEventLoopGroup(eventLoopGroup);
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
  public static class Builder extends AbstractCluster.Builder<Builder, NettyLocalMember, NettyRemoteMember> {
    private EventLoopGroup eventLoopGroup;

    private Builder() {
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
      return new NettyCluster(eventLoopGroup, localMember, remoteMembers);
    }
  }

}
