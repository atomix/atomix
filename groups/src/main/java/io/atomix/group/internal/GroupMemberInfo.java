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
package io.atomix.group.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

/**
 * Group member info.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMemberInfo implements CatalystSerializable {
  private long index;
  private String memberId;
  private Object metadata;

  public GroupMemberInfo() {
  }

  public GroupMemberInfo(long index, String memberId, Object metadata) {
    this.index = Assert.argNot(index, index <= 0, "index must be positive");
    this.memberId = Assert.notNull(memberId, "memberId");
    this.metadata = metadata;
  }

  /**
   * Returns the member index.
   *
   * @return The member index.
   */
  long index() {
    return index;
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  public String memberId() {
    return memberId;
  }

  public Object metadata() {
    return metadata;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(index).writeString(memberId);
    serializer.writeObject(metadata, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    index = buffer.readLong();
    memberId = buffer.readString();
    metadata = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
