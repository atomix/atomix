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
 * Member join/leave event.<p>
 *
 * When a {@link net.kuujo.copycat.cluster.Member.Type#PASSIVE} member joins or leaves the cluster, a {@code MembershipEvent} will be triggered,
 * allowing the user to react to the membership change. Membership change events can be observed by adding a
 * an {@link net.kuujo.copycat.EventListener} to a member set via
 * {@link net.kuujo.copycat.cluster.Cluster#addMembershipListener(net.kuujo.copycat.EventListener)}.<p>
 *
 * <pre>
 *   {@code
 *     cluster.addMembershipListener(event -> {
 *       if (event.type() == MembershipEvent.Type.JOIN) {
 *         event.member().send("Hello!").thenAccept(reply -> {
 *           System.out.println(event.member().id() + " said " + reply);
 *         });
 *       }
 *     });
 *   }
 * </pre>
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
  private final Member member;

  public MembershipChangeEvent(Type type, Member member) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    if (member == null)
      throw new NullPointerException("member cannot be null");
    this.type = type;
    this.member = member;
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
  public Member member() {
    return member;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof MembershipChangeEvent && ((MembershipChangeEvent) object).type == type && ((MembershipChangeEvent) object).member.equals(member);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, member);
  }

  @Override
  public String toString() {
    return String.format("%s[type=%s, member=%s]", getClass().getSimpleName(), type, member);
  }

}
