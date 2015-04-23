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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.Event;
import net.kuujo.copycat.cluster.Member;

/**
 * Leader change event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderChangeEvent implements Event {
  private final Member oldLeader;
  private final Member newLeader;

  public LeaderChangeEvent(Member oldLeader, Member newLeader) {
    this.oldLeader = oldLeader;
    this.newLeader = newLeader;
  }

  /**
   * Returns the old leader.
   *
   * @return The old leader or {@code null} if no old leader existed.
   */
  public Member oldLeader() {
    return oldLeader;
  }

  /**
   * Returns the new leader.
   *
   * @return The new leader or {@code null} if no leader existed.
   */
  public Member newLeader() {
    return newLeader;
  }

}
