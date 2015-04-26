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
public class TestCluster extends AbstractCluster {

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final TestMemberRegistry registry;

  public TestCluster(TestLocalMember localMember, Collection<? extends TestRemoteMember> remoteMembers, TestMemberRegistry registry) {
    super(localMember, remoteMembers);
    this.registry = registry;
    localMember.init(registry);
    remoteMembers.forEach(m -> m.init(registry));
  }

  @Override
  protected AbstractRemoteMember createRemoteMember(AbstractMember.Info info) {
    return new TestRemoteMember((TestMember.Info) info, localMember.serializer.copy(), new ExecutionContext(String.format("copycat-cluster-%d", info.id()))).init(registry);
  }

  @Override
  public TestMember member(int id) {
    return (TestMember) super.member(id);
  }

  /**
   * Partitions members from the given member.
   */
  public void partition(int id) {
    if (localMember.id() == id) {
      remoteMembers.values().forEach(m -> ((TestRemoteMember) m).partition());
    } else {
      TestRemoteMember member = (TestRemoteMember) remoteMembers.get(id);
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
      remoteMembers.values().forEach(m -> ((TestRemoteMember) m).heal());
    } else {
      TestRemoteMember member = (TestRemoteMember) remoteMembers.get(id);
      if (member != null) {
        member.heal();
      }
    }
  }

  /**
   * Raft test cluster builder.
   */
  public static class Builder extends AbstractCluster.Builder<Builder, TestLocalMember, TestRemoteMember> {
    private TestMemberRegistry registry;

    private Builder() {
    }

    /**
     * Sets the test member registry.
     *
     * @param registry The test member registry.
     * @return The test cluster builder.
     */
    public Builder withRegistry(TestMemberRegistry registry) {
      this.registry = registry;
      return this;
    }

    @Override
    public TestCluster build() {
      if (registry == null)
        throw new ConfigurationException("member registry must be provided");
      return new TestCluster(localMember, remoteMembers, registry);
    }
  }

}
