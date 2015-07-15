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
package net.kuujo.copycat.raft.log;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.log.Entry;

/**
 * Member info entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class MemberEntry<T extends MemberEntry<T>> extends RaftEntry<T> {
  private Member member;

  protected MemberEntry() {
  }

  protected MemberEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the member info.
   *
   * @return The member info.
   */
  public Member getMember() {
    return member;
  }

  /**
   * Sets the member info.
   *
   * @param member The member info.
   * @return The member entry.
   */
  @SuppressWarnings("unchecked")
  public T setMember(Member member) {
    if (member == null)
      throw new NullPointerException("member cannot be null");
    this.member = member;
    return (T) this;
  }

  @Override
  public int size() {
    return super.size() + Integer.BYTES;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    super.writeObject(buffer, alleycat);
    alleycat.writeObject(member, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    super.readObject(buffer, alleycat);
    member = alleycat.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%s]", getClass().getSimpleName(), getIndex(), getTerm(), member);
  }

}
