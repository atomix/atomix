// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.AbstractEvent;

import java.util.Collection;

/**
 * Member group event.
 */
public class MemberGroupEvent extends AbstractEvent<MemberGroupEvent.Type, Collection<MemberGroup>> {

  /**
   * Member group event type.
   */
  public enum Type {
    GROUPS_CHANGED
  }

  public MemberGroupEvent(Type type, Collection<MemberGroup> subject) {
    super(type, subject);
  }

  public MemberGroupEvent(Type type, Collection<MemberGroup> subject, long time) {
    super(type, subject, time);
  }
}
