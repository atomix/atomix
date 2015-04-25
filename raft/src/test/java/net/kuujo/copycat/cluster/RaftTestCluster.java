package net.kuujo.copycat.cluster;/*
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

import net.kuujo.copycat.util.ExecutionContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Raft test cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftTestCluster extends AbstractCluster {
  private final Map<String, RaftTestLocalMember> registry = new ConcurrentHashMap<>();

  public RaftTestCluster(RaftTestLocalMember localMember, Collection<? extends RaftTestRemoteMember> remoteMembers) {
    super(localMember, remoteMembers);
    localMember.init(registry);
    remoteMembers.forEach(m -> m.init(registry));
  }

  @Override
  protected AbstractRemoteMember createRemoteMember(AbstractMember.Info info) {
    return new RaftTestRemoteMember((RaftTestMember.Info) info, localMember.serializer.copy(), new ExecutionContext(String.format("copycat-cluster-%d", info.id()))).init(registry);
  }

  /**
   * Raft test cluster builder.
   */
  public static class Builder extends AbstractCluster.Builder<Builder, RaftTestLocalMember, RaftTestRemoteMember> {

    private Builder() {
    }

    @Override
    public ManagedCluster build() {
      return new RaftTestCluster(localMember, remoteMembers);
    }
  }

}
