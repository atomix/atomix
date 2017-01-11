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

import io.atomix.copycat.server.session.ServerSession;

/**
 * Group session state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class SessionState {
  private final ServerSession session;

  SessionState(ServerSession session) {
    this.session = session;
  }

  /**
   * Returns the session ID.
   */
  public long id() {
    return session.id();
  }

  /**
   * Sends a join event to the session for the given member.
   */
  public void join(MemberState member) {
    if (session.state().active()) {
      session.publish("join", member.info());
    }
  }

  /**
   * Indicates that the given member's status has changed to ALIVE.
   */
  public void alive(MemberState member) {
    if (session.state().active()) {
      session.publish("alive", member.id());
    }
  }

  /**
   * Indicates that the given member's status has changed to DEAD.
   */
  public void dead(MemberState member) {
    if (session.state().active()) {
      session.publish("dead", member.id());
    }
  }

  /**
   * Sends a leave event to the session for the given member.
   */
  public void leave(MemberState member) {
    if (session.state().active()) {
      session.publish("leave", member.id());
    }
  }

  /**
   * Sends a term event to the session for the given member.
   */
  public void term(long term) {
    if (session.state().active()) {
      session.publish("term", term);
    }
  }

  /**
   * Sends an elect event to the session for the given member.
   */
  public void elect(MemberState member) {
    if (session.state().active()) {
      session.publish("elect", member.id());
    }
  }

  @Override
  public int hashCode() {
    return session.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof SessionState && ((SessionState) object).session.equals(session);
  }

}
