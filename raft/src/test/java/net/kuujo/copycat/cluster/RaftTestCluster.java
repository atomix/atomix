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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.Collection;

/**
 * Raft test cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftTestCluster extends AbstractCluster {

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final RaftTestMemberRegistry registry;

  public RaftTestCluster(RaftTestLocalMember localMember, Collection<? extends RaftTestRemoteMember> remoteMembers, RaftTestMemberRegistry registry) {
    super(localMember, remoteMembers);
    this.registry = registry;
    localMember.init(registry);
    remoteMembers.forEach(m -> m.init(registry));
  }

  @Override
  protected AbstractRemoteMember createRemoteMember(AbstractMember.Info info) {
    return new RaftTestRemoteMember((RaftTestMember.Info) info, localMember.serializer.copy(), new ExecutionContext(String.format("copycat-cluster-%d", info.id()))).init(registry);
  }

  @Override
  public RaftTestMember member(int id) {
    return (RaftTestMember) super.member(id);
  }

  /**
   * Partitions members from the given member.
   */
  public void partition(int id) {
    if (localMember.id() == id) {
      remoteMembers.values().forEach(m -> ((RaftTestRemoteMember) m).partition());
    } else {
      RaftTestRemoteMember member = (RaftTestRemoteMember) remoteMembers.get(id);
      if (member != null) {
        member.partition();
      }
    }
  }

  /**
   * Heals a partition for the given member.
   */
  public void heal(int id) {
    if (localMember.id() == id) {
      remoteMembers.values().forEach(m -> ((RaftTestRemoteMember) m).heal());
    } else {
      RaftTestRemoteMember member = (RaftTestRemoteMember) remoteMembers.get(id);
      if (member != null) {
        member.heal();
      }
    }
  }

  /**
   * Raft test cluster builder.
   */
  public static class Builder extends AbstractCluster.Builder<Builder, RaftTestLocalMember, RaftTestRemoteMember> {
    private RaftTestMemberRegistry registry;

    private Builder() {
    }

    /**
     * Sets the test member registry.
     *
     * @param registry The test member registry.
     * @return The test cluster builder.
     */
    public Builder withRegistry(RaftTestMemberRegistry registry) {
      this.registry = registry;
      return this;
    }

    @Override
    public RaftTestCluster build() {
      if (registry == null)
        throw new ConfigurationException("member registry must be provided");
      return new RaftTestCluster(localMember, remoteMembers, registry);
    }
  }

}
