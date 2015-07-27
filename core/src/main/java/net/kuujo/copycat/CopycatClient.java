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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.copycat.client.RaftClient;
import net.kuujo.copycat.transport.Transport;

import java.util.concurrent.TimeUnit;

/**
 * Client-side {@link net.kuujo.copycat.Copycat} implementation.
 * <p>
 * This is a {@link net.kuujo.copycat.Copycat} implementation that executes all {@link net.kuujo.copycat.Resource} operations
 * remotely via a {@link net.kuujo.copycat.client.RaftClient}.
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

  private CopycatClient(RaftClient client) {
    super(client);
  }

  /**
   * Client builder.
   */
  public static class Builder extends Copycat.Builder<CopycatClient> {
    private RaftClient.Builder builder = RaftClient.builder();

    private Builder() {
    }

    @Override
    protected void reset() {
      builder = RaftClient.builder();
    }

    /**
     * Sets the client transport.
     *
     * @param transport The client transport.
     * @return The client builder.
     */
    public Builder withTransport(Transport transport) {
      builder.withTransport(transport);
      return this;
    }

    /**
     * Sets the client serializer.
     *
     * @param serializer The client serializer.
     * @return The client builder.
     */
    public Builder withSerializer(Alleycat serializer) {
      builder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval) {
      builder.withKeepAliveInterval(keepAliveInterval);
      return this;
    }

    /**
     * Sets the interval at which to send keep alive requests.
     *
     * @param keepAliveInterval The interval at which to send keep alive requests.
     * @param unit The keep alive interval time unit.
     * @return The client builder.
     */
    public Builder withKeepAliveInterval(long keepAliveInterval, TimeUnit unit) {
      builder.withKeepAliveInterval(keepAliveInterval, unit);
      return this;
    }

    /**
     * Sets the client seed members.
     *
     * @param members The client seed members.
     * @return The client builder.
     */
    public Builder withMembers(Members members) {
      builder.withMembers(members);
      return this;
    }

    @Override
    public CopycatClient build() {
      return new CopycatClient(builder.build());
    }
  }

}
