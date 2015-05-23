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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.Event;

import java.util.Objects;

/**
 * Member join/leave event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MembershipChangeEvent implements Event {

  /**
   * Membership event type.
   */
  public static enum Type {
    JOIN,
    LEAVE
  }

  private final Type type;
  private final MemberInfo info;

  public MembershipChangeEvent(Type type, MemberInfo info) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    if (info == null)
      throw new NullPointerException("info cannot be null");
    this.type = type;
    this.info = info;
  }

  /**
   * Returns the membership change event type.
   *
   * @return The membership change event type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the member on which this event occurred.
   *
   * @return The event member.
   */
  public MemberInfo info() {
    return info;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof MembershipChangeEvent && ((MembershipChangeEvent) object).type == type && ((MembershipChangeEvent) object).info.equals(info);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, info);
  }

  @Override
  public String toString() {
    return String.format("%s[type=%s, member=%s]", getClass().getSimpleName(), type, info);
  }

}
