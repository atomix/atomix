// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/**
 * Leader elector events.
 */
public enum PrimaryElectorEvents implements EventType {
  CHANGE("change");

  private final String id;

  PrimaryElectorEvents(String id) {
    this.id = id;
  }

  @Override
  public String id() {
    return id;
  }

  public static final Namespace NAMESPACE = Namespace.builder()
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 50)
      .register(PrimaryElectionEvent.class)
      .register(PrimaryElectionEvent.Type.class)
      .register(PrimaryTerm.class)
      .register(GroupMember.class)
      .register(MemberId.class)
      .register(MemberGroupId.class)
      .register(PartitionId.class)
      .build(PrimaryElectorEvents.class.getSimpleName());
}
