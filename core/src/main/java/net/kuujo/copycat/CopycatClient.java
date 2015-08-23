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

import net.kuujo.copycat.raft.RaftClient;

import java.time.Duration;

/**
 * Client-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that executes all {@link net.kuujo.copycat.Resource} operations
 * remotely via a {@link RaftClient}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatClient extends Copycat {

  /**
   * Returns a new Copycat client builder.
   *
   * @return A new Copycat client builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CopycatClient(RaftClient client) {
    super(client);
  }

  /**
   * Client builder.
   */
  public static class Builder extends Copycat.Builder {
    private Builder() {
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
