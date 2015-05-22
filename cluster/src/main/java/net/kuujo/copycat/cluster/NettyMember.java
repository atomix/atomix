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
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ExecutionContext;

import java.net.InetSocketAddress;

/**
 * Netty cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NettyMember extends ManagedMember {

  /**
   * Returns a new Netty member builder.
   *
   * @return A new Netty member builder.
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  InetSocketAddress address();

  /**
   * Netty member info.
   */
  static class Info extends AbstractMember.Info {
    InetSocketAddress address;

    public Info() {
    }

    public Info(int id, Type type, InetSocketAddress address) {
      super(id, type);
      this.address = address;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeInt(address.getHostString().getBytes().length)
        .write(address.getHostString().getBytes())
        .writeInt(address.getPort());
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      address = new InetSocketAddress(new String(bytes), buffer.readInt());
    }
  }

  /**
   * Netty remote member builder.
   */
  public static class Builder extends AbstractMember.Builder<Builder, NettyMember> {
    private String host;
    private int port;

    /**
     * Sets the member host.
     *
     * @param host The member host.
     * @return The member builder.
     */
    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    /**
     * Sets the member port.
     *
     * @param port The member port.
     * @return The member builder.
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    @Override
    public NettyRemoteMember build() {
      if (id <= 0)
        throw new ConfigurationException("member id must be greater than 0");
      return new NettyRemoteMember(new NettyMember.Info(id, Type.ACTIVE, new InetSocketAddress(host != null ? host : "localhost", port)), new ExecutionContext(String.format("copycat-cluster-%d", id)));
    }
  }

}
