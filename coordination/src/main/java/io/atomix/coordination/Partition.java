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
package io.atomix.coordination;

import io.atomix.catalyst.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a segment of the cluster managed by consistent hashing.
 * <p>
 * {@link DistributedGroup} members are split into segments called partition. A partition represents
 * a group of neighboring {@link GroupMember members} that may participate in replication or messaging
 * protocols outside the core consensus service. The number of members in a partition is dependent both
 * on the group {@link DistributedGroup.Config configuration} and the number of members in the group.
 * In particular, the {@link DistributedGroup.Config#withReplicationFactor(int) replication factor} controls
 * the number of members in each partition.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Partition implements Iterable<GroupMember> {
  private final int id;
  private volatile Collection<GroupMember> members = new ArrayList<>();

  Partition(int id) {
    this.id = id;
  }

  /**
   * Returns the partition ID.
   *
   * @return The partition ID.
   */
  public int id() {
    return id;
  }

  /**
   * Updates the partition with the given number of group members.
   */
  void update(Collection<GroupMember> members) {
    this.members = Assert.notNull(members, "members");
  }

  /**
   * Returns a collection of members for the partition.
   *
   * @return A collection of members for the partition.
   */
  public Collection<GroupMember> members() {
    return members;
  }

  @Override
  public Iterator<GroupMember> iterator() {
    return members.iterator();
  }

}
