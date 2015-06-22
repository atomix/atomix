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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.io.Buffer;
import net.kuujo.copycat.ConfigurationException;

/**
 * Raft test member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface TestMember extends Member {

  /**
   * Returns a new test member builder.
   *
   * @return A new test member builder.
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  String address();

  /**
   * Test member info.
   */
  static class Info extends MemberInfo {
    String address;

    public Info() {
    }

    public Info(int id, String address) {
      super(id);
      this.address = address;
    }

    @Override
    public void writeObject(Buffer buffer, Alleycat alleycat) {
      super.writeObject(buffer, alleycat);
      buffer.writeInt(address.getBytes().length).write(address.getBytes());
    }

    @Override
    public void readObject(Buffer buffer, Alleycat alleycat) {
      super.readObject(buffer, alleycat);
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      address = new String(bytes);
    }

    @Override
    public String toString() {
      return String.format("%s[id=%d, address=%s]", getClass().getSimpleName(), id(), address);
    }
  }

  /**
   * Raft test remote member builder.
   */
  public static class Builder extends ManagedMember.Builder<Builder, TestRemoteMember> {
    private String address;

    /**
     * Sets the member address.
     *
     * @param address The member address.
     * @return The member builder.
     */
    public Builder withAddress(String address) {
      this.address = address;
      return this;
    }

    @Override
    public TestRemoteMember build() {
      if (id <= 0)
        throw new ConfigurationException("member id must be greater than 0");
      if (address == null)
        throw new ConfigurationException("address cannot be null");
      return new TestRemoteMember(new TestMember.Info(id, address), Type.ACTIVE);
    }
  }

}
