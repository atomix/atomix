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
package io.atomix.coordination;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;

/**
 * A {@link DistributedGroup} member representing a member of the group controlled by a local
 * or remote process.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMember {
  protected final String memberId;
  protected final Address address;
  protected final DistributedGroup group;
  private final GroupProperties properties;
  private final GroupTaskQueue tasks;
  private final GroupScheduler scheduler;
  private final GroupConnection connection;

  GroupMember(GroupMemberInfo info, DistributedGroup group) {
    this.memberId = info.memberId();
    this.address = info.address();
    this.group = Assert.notNull(group, "group");
    this.properties = new GroupProperties(memberId, group);
    this.tasks = new GroupTaskQueue(memberId, group);
    this.scheduler = new GroupScheduler(memberId, group);
    this.connection = new GroupConnection(memberId, address, group.connections);
  }

  /**
   * Returns the member ID.
   * <p>
   * The member ID is guaranteed to be unique across the cluster. Depending on how the member was
   * constructed, it may be a user-provided identifier or an automatically generated {@link java.util.UUID}.
   *
   * @return The member ID.
   */
  public String id() {
    return memberId;
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  public Address address() {
    return address;
  }

  /**
   * Returns a boolean value indicating whether this member is the current leader.
   * <p>
   * Whether this member is the current leader is dependent on the last known configuration of the membership
   * group. If the local group instance is partitioned from the cluster, it may believe a member to be the
   * leader when it has in fact been replaced.
   *
   * @return Indicates whether this member is the current leader.
   */
  public boolean isLeader() {
    return group.election().leader != null && group.election().leader.equals(memberId);
  }

  public GroupProperties properties() {
    return properties;
  }

  public GroupConnection connection() {
    return connection;
  }

  public GroupTaskQueue tasks() {
    return tasks;
  }

  public GroupScheduler scheduler() {
    return scheduler;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, address=%s]", getClass().getSimpleName(), memberId, address);
  }

}
