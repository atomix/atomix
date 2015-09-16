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

import net.kuujo.catalog.client.RaftClient;
import net.kuujo.catalyst.transport.Address;
import net.kuujo.catalyst.util.Assert;

import java.util.Arrays;
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
  public static Builder builder(Address... members) {
    return new Builder(Arrays.asList(Assert.notNull(members, "members")));
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
  public static Builder builder(Collection<Address> members) {
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
    private Builder(Collection<Address> members) {
      super(members);
    }

    @Override
    public CopycatClient build() {
      return new CopycatClient(clientBuilder.build());
    }
  }

}
