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

import net.kuujo.copycat.util.ExecutionContext;

import java.util.Collection;

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

  public NettyCluster(NettyLocalMember localMember, Collection<NettyRemoteMember> remoteMembers) {
    super(localMember, remoteMembers);
  }

  @Override
  protected AbstractRemoteMember createRemoteMember(AbstractMember.Info info) {
    return new NettyRemoteMember(info.address.substring(0, info.address.indexOf(':')), Integer.valueOf(info.address.substring(info.address.indexOf(':') + 1)), info, localMember.serializer.copy(), new ExecutionContext(String.format("copycat-cluster-%d", info.id())));
  }

  /**
   * Netty cluster builder.
   */
  public static class Builder extends AbstractCluster.Builder<NettyLocalMember, NettyRemoteMember> {
    private Builder() {
    }

    @Override
    public ManagedCluster build() {
      return new NettyCluster(localMember, remoteMembers);
    }
  }

}
