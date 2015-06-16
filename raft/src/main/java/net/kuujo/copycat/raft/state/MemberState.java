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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.cluster.Member;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MemberState {
  private final int id;
  private final Member.Type type;
  private long timestamp;
  private long version;
  private int index;
  private Member.Status status = Member.Status.ALIVE;
  private long matchIndex;
  private long nextIndex;

  public MemberState(int id, Member.Type type, long timestamp) {
    this.id = id;
    this.type = type;
    this.timestamp = timestamp;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  int getId() {
    return id;
  }

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  Member.Type getType() {
    return type;
  }

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  Member.Status getStatus() {
    return status;
  }

  /**
   * Returns the member version.
   *
   * @return The member version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the member version.
   *
   * @param version The member version.
   * @return The member state.
   */
  MemberState setVersion(long version) {
    this.version = version;
    return this;
  }

  /**
   * Returns the member index.
   *
   * @return The member index.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Sets the member index.
   *
   * @param index The member index.
   * @return The member state.
   */
  MemberState setIndex(int index) {
    return this;
  }

  /**
   * Returns the member's match index.
   *
   * @return The member's match index.
   */
  long getMatchIndex() {
    return matchIndex;
  }

  /**
   * Sets the member's match index.
   *
   * @param matchIndex The member's match index.
   * @return The member state.
   */
  MemberState setMatchIndex(long matchIndex) {
    this.matchIndex = matchIndex;
    return this;
  }

  /**
   * Returns the member's next index.
   *
   * @return The member's next index.
   */
  long getNextIndex() {
    return nextIndex;
  }

  /**
   * Sets the member's next index.
   *
   * @param nextIndex The member's next index.
   * @return The member state.
   */
  MemberState setNextIndex(long nextIndex) {
    this.nextIndex = nextIndex;
    return this;
  }

  /**
   * Updates the member.
   */
  public boolean update(long timestamp, long timeout) {
    if (timestamp - timeout > this.timestamp) {
      this.timestamp = timestamp;
      status = Member.Status.DEAD;
      return false;
    } else {
      this.timestamp = timestamp;
      status = Member.Status.ALIVE;
      return true;
    }
  }

}
