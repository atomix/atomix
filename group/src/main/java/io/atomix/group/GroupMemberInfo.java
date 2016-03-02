/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;

/**
 * Group member info.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMemberInfo implements CatalystSerializable {
  private String memberId;
  private Address address;

  public GroupMemberInfo() {
  }

  public GroupMemberInfo(String memberId, Address address) {
    this.memberId = Assert.notNull(memberId, "memberId");
    this.address = address;
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  public String memberId() {
    return memberId;
  }

  /**
   * Returns the member server address.
   *
   * @return The member address, or {@code null} if the member wasn't configured with a server address.
   */
  public Address address() {
    return address;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeString(memberId);
    serializer.writeObject(address, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    memberId = buffer.readString();
    address = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s, address=%s]", getClass().getSimpleName(), memberId, address);
  }

}
