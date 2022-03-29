// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * A partition or shard is a group of controller nodes that are work together to maintain state.
 * A ONOS cluster is typically made of of one or partitions over which the the data is partitioned.
 */
public class PartitionMetadata {
  private final PartitionId id;
  private final Collection<MemberId> members;

  public PartitionMetadata(PartitionId id, Collection<MemberId> members) {
    this.id = id;
    this.members = members;
  }

  /**
   * Returns the partition identifier.
   *
   * @return partition identifier
   */
  public PartitionId id() {
    return id;
  }

  /**
   * Returns the controller nodes that are members of this partition.
   *
   * @return collection of controller node identifiers
   */
  public Collection<MemberId> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PartitionMetadata) {
      PartitionMetadata partition = (PartitionMetadata) object;
      return partition.id.equals(id) && partition.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("members", members)
        .toString();
  }
}
