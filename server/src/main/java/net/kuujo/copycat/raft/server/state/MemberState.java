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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.raft.Member;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MemberState {
  private final Member member;
  private long timestamp;
  private int index;
  private long session;
  private long matchIndex;
  private long nextIndex;

  public MemberState(Member member, long timestamp) {
    if (member == null)
      throw new NullPointerException("member cannot be null");

    this.member = member;
    this.timestamp = timestamp;
  }

  /**
   * Returns the member configuration.
   *
   * @return The member configuration.
   */
  public Member getMember() {
    return member;
  }

  /**
   * Returns the member session.
   *
   * @return The member session.
   */
  public long getSession() {
    return session;
  }

  /**
   * Sets the member session.
   *
   * @param session The member session.
   * @return The member state.
   */
  MemberState setSession(long session) {
    this.session = session;
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
    this.index = index;
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
      return false;
    } else {
      this.timestamp = timestamp;
      return true;
    }
  }

}
