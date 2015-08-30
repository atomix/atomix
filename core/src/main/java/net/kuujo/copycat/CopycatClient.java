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
package net.kuujo.copycat;

import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.RaftClient;

import java.time.Duration;
import java.util.Collection;

/**
 * Client-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that executes all {@link net.kuujo.copycat.Resource} operations
 * remotely via a {@link RaftClient}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class CopycatClient extends Copycat {

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Member... members) {
    return new Builder(Members.builder().withMembers(members).build());
  }

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Collection<Member> members) {
    return new Builder(Members.builder().withMembers(members).build());
  }

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Members members) {
    return new Builder(members);
  }

  /**
   * @throws NullPointerException if {@code client} is null
   */
  public CopycatClient(RaftClient client) {
    super(client);
  }

  /**
   * Client builder.
   */
  public static class Builder extends Copycat.Builder {
    private Builder(Members members) {
      super(members);
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(Duration keepAliveInterval) {
      clientBuilder.withKeepAliveInterval(keepAliveInterval);
      return this;
    }

    @Override
    public CopycatClient build() {
      return new CopycatClient(clientBuilder.build());
    }
  }

}
