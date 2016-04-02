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

import io.atomix.group.Member;
import io.atomix.group.messaging.internal.MemberMessageClient;

/**
 * Group member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMember extends AbstractGroupMember implements Member {
  private final MemberMessageClient messages;

  public GroupMember(GroupMemberInfo info, MembershipGroup group, GroupSubmitter submitter) {
    super(info, group);
    this.messages = new MemberMessageClient(this, submitter);
  }

  @Override
  public MemberMessageClient messages() {
    return messages;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof GroupMember && ((GroupMember) object).memberId.equals(memberId);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), memberId);
  }

}
