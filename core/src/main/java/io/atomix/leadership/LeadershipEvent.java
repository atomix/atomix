/*
 * Copyright 2014-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.leadership;

import com.google.common.base.MoreObjects;
import io.atomix.event.AbstractEvent;

import java.util.Objects;

/**
 * Describes leadership-related event.
 */
public class LeadershipEvent extends AbstractEvent<LeadershipEvent.Type, Leadership> {

  /**
   * Type of leadership events.
   */
  public enum Type {
    /**
     * Signifies a change in both the leader as well as change to the list of candidates. Keep in mind though that
     * the first node entering the race will trigger this event as it will become a candidate and automatically get
     * promoted to become leader.
     */
    LEADER_AND_CANDIDATES_CHANGED,

    /**
     * Signifies that the leader for a topic has changed.
     */
    // TODO: We may not need this. We currently do not support a way for a current leader to step down
    // while still remaining a candidate
    LEADER_CHANGED,

    /**
     * Signifies a change in the list of candidates for a topic.
     */
    CANDIDATES_CHANGED,

    /**
     * Signifies the Leadership Elector is unavailable.
     */
    SERVICE_DISRUPTED,

    /**
     * Signifies the Leadership Elector is available again.
     */
    SERVICE_RESTORED
  }

  /**
   * Creates an event of a given type and for the specified instance and the
   * current time.
   *
   * @param type       leadership event type
   * @param leadership event subject
   */
  public LeadershipEvent(Type type, Leadership leadership) {
    super(type, leadership);
  }

  /**
   * Creates an event of a given type and for the specified subject and time.
   *
   * @param type       leadership event type
   * @param leadership event subject
   * @param time       occurrence time
   */
  public LeadershipEvent(Type type, Leadership leadership, long time) {
    super(type, leadership, time);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type(), subject(), time());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof LeadershipEvent) {
      final LeadershipEvent other = (LeadershipEvent) obj;
      return Objects.equals(this.type(), other.type()) &&
          Objects.equals(this.subject(), other.subject()) &&
          Objects.equals(this.time(), other.time());
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("type", type())
        .add("subject", subject())
        .add("time", time())
        .toString();
  }
}
