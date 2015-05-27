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
package net.kuujo.copycat.raft.log.entry;

import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Member info entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class MemberEntry<T extends MemberEntry<T>> extends RaftEntry<T> {
  private MemberInfo member;

  protected MemberEntry() {
  }

  protected MemberEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the member info.
   *
   * @return The member info.
   */
  public MemberInfo getMember() {
    return member;
  }

  /**
   * Sets the member info.
   *
   * @param member The member info.
   * @return The member entry.
   */
  @SuppressWarnings("unchecked")
  public T setMember(MemberInfo member) {
    if (member == null)
      throw new NullPointerException("member cannot be null");
    this.member = member;
    return (T) this;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(member, buffer);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    member = serializer.readObject(buffer);
  }

}
