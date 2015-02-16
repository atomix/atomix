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
import net.kuujo.copycat.util.internal.Assert;

import java.util.Objects;
import java.util.UUID;

/**
 * Member join/leave event.<p>
 *
 * When a {@link Member.Type#PASSIVE} member joins or leaves the cluster, a {@code MembershipEvent} will be triggered,
 * allowing the user to react to the membership change. Membership change events can be observed by adding a
 * an {@link net.kuujo.copycat.EventListener} to a member set via
 * {@link net.kuujo.copycat.cluster.Cluster#addMembershipListener(net.kuujo.copycat.EventListener)} or
 * {@link net.kuujo.copycat.cluster.Members#addListener(net.kuujo.copycat.EventListener)}.<p>
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
public class MembershipEvent implements Event<MembershipEvent.Type> {

  /**
   * Membership event type.
   */
  public static enum Type {
    JOIN,
    LEAVE
  }

  private final String id = UUID.randomUUID().toString();
  private final Type type;
  private final Member member;

  public MembershipEvent(Type type, Member member) {
    this.type = Assert.isNotNull(type, "type");
    this.member = Assert.isNotNull(member, "member");
  }

  @Override
  public String id() {
    return id;
  }

  @Override
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
    return object instanceof MembershipEvent && ((MembershipEvent) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, member);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, type=%s, member=%s]", getClass().getSimpleName(), id, type, member);
  }

}
