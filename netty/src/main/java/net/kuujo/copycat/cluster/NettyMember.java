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

import net.kuujo.copycat.io.Buffer;

import java.net.InetSocketAddress;

/**
 * Netty cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NettyMember extends Member {

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
    public void writeObject(Buffer buffer) {
      super.writeObject(buffer);
      buffer.writeInt(address.getHostString().getBytes().length)
        .write(address.getHostString().getBytes())
        .writeInt(address.getPort());
    }

    @Override
    public void readObject(Buffer buffer) {
      super.readObject(buffer);
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      address = new InetSocketAddress(new String(bytes), buffer.readInt());
    }
  }

}
