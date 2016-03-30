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

import io.atomix.group.GroupMemberInfo;
import io.atomix.group.Member;
import io.atomix.group.MembershipGroup;
import io.atomix.group.messaging.internal.ConnectionManager;
import io.atomix.group.messaging.internal.MemberMessageClient;
import io.atomix.group.task.internal.MemberTaskClient;
import io.atomix.group.util.Submitter;

/**
 * Group member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMember extends AbstractGroupMember implements Member {
  private final MemberMessageClient messages;
  private final MemberTaskClient tasks;

  public GroupMember(GroupMemberInfo info, MembershipGroup group, Submitter submitter, ConnectionManager connections) {
    super(info, group);
    this.messages = new MemberMessageClient(this, connections);
    this.tasks = new MemberTaskClient(this, submitter);
  }

  /**
   * Returns the member message service.
   *
   * @return The member message service.
   */
  public MemberMessageClient messages() {
    return messages;
  }

  /**
   * Returns the member task service.
   *
   * @return The member task service.
   */
  public MemberTaskClient tasks() {
    return tasks;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof GroupMember && ((GroupMember) object).memberId.equals(memberId);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, address=%s]", getClass().getSimpleName(), memberId, address);
  }

}
