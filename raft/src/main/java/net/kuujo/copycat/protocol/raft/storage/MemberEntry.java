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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Member entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberEntry<T extends MemberEntry<T>> extends TimestampedEntry<T> {
  private int member;

  public MemberEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry member.
   *
   * @return The entry member.
   */
  public int getMember() {
    return member;
  }

  /**
   * Sets the entry member.
   *
   * @param member The entry member.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setMember(int member) {
    this.member = member;
    return (T) this;
  }


  @Override
  public int size() {
    return super.size() + 4;
  }

  @Override
  public void writeObject(Buffer buffer) {
    super.writeObject(buffer);
    buffer.writeInt(member);
  }

  @Override
  public void readObject(Buffer buffer) {
    super.readObject(buffer);
    member = buffer.readInt();
  }

}
