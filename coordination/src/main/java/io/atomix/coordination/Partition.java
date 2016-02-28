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
 * Group partition.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Partition implements Iterable<GroupMember> {
  private volatile Collection<GroupMember> members = new ArrayList<>();

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
