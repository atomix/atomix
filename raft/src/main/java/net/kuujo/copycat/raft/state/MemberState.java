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
  private long matchIndex;
  private long nextIndex;
  private long timestamp;
  private long commitTime;

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
   * Returns the commit time.
   *
   * @return The commit time.
   */
  long getCommitTime() {
    return commitTime;
  }

  /**
   * Sets the commit time.
   *
   * @param commitTime The commit time.
   * @return The member state.
   */
  MemberState setCommitTime(long commitTime) {
    this.commitTime = commitTime;
    return this;
  }

  /**
   * Updates the member.
   */
  public boolean update(long timestamp, long timeout) {
    if (timestamp - timeout > this.timestamp) {
      this.timestamp = timestamp;
      return false;
    }
    this.timestamp = timestamp;
    return true;
  }

}
