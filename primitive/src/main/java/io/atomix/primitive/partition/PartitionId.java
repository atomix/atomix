/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitive.partition;

import com.google.common.base.Preconditions;
import io.atomix.utils.AbstractIdentifier;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link PartitionMetadata} identifier.
 */
public class PartitionId extends AbstractIdentifier<Integer> implements Comparable<PartitionId> {
  private final String group;

  /**
   * Creates a partition identifier from an integer.
   *
   * @param group the group identifier
   * @param id input integer
   */
  public PartitionId(String group, int id) {
    super(id);
    this.group = checkNotNull(group, "group cannot be null");
    Preconditions.checkArgument(id >= 0, "partition id must be non-negative");
  }

  /**
   * Creates a partition identifier from an integer.
   *
   * @param group the group identifier
   * @param id input integer
   * @return partition identification
   */
  public static PartitionId from(String group, int id) {
    return new PartitionId(group, id);
  }

  @Override
  public int compareTo(PartitionId that) {
    return Integer.compare(this.identifier, that.identifier);
  }

  /**
   * Returns the partition group name.
   *
   * @return the partition group name
   */
  public String group() {
    return group;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id(), group());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PartitionId) {
      PartitionId partitionId = (PartitionId) object;
      return partitionId.id().equals(id()) && partitionId.group().equals(group());
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .add("group", group)
        .toString();
  }
}
