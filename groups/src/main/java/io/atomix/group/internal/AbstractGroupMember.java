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

import io.atomix.catalyst.util.Assert;
import io.atomix.group.GroupMember;
import io.atomix.group.messaging.internal.AbstractMessageClient;

/**
 * Abstract group member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractGroupMember implements GroupMember {
  protected final String memberId;
  protected final MembershipGroup group;

  public AbstractGroupMember(GroupMemberInfo info, MembershipGroup group) {
    this.memberId = info.memberId();
    this.group = Assert.notNull(group, "group");
  }

  @Override
  public String id() {
    return memberId;
  }

  @Override
  public abstract AbstractMessageClient messages();

}
