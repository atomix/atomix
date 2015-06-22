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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.Buffer;
import net.kuujo.alleycat.util.ReferenceManager;

/**
 * Heart beat entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=305)
public class HeartbeatEntry extends TimestampedEntry<HeartbeatEntry> {
  private int memberId;

  public HeartbeatEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Sets the heartbeat member ID.
   *
   * @param memberId The member ID.
   * @return The heartbeat entry.
   */
  public HeartbeatEntry setMemberId(int memberId) {
    this.memberId = memberId;
    return this;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int getMemberId() {
    return memberId;
  }

  @Override
  public int size() {
    return super.size() + Integer.BYTES;
  }

  @Override
  public void writeObject(Buffer buffer, Alleycat alleycat) {
    super.writeObject(buffer, alleycat);
    buffer.writeInt(memberId);
  }

  @Override
  public void readObject(Buffer buffer, Alleycat alleycat) {
    super.readObject(buffer, alleycat);
    memberId = buffer.readInt();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%d, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), memberId, getTimestamp());
  }

}
