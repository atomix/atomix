// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.AbstractEvent;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary election event.
 */
public class PrimaryElectionEvent extends AbstractEvent<PrimaryElectionEvent.Type, PrimaryTerm> {

  /**
   * Returns the election event type.
   */
  public enum Type {
    CHANGED
  }

  private final PartitionId partitionId;

  public PrimaryElectionEvent(Type type, PartitionId partitionId, PrimaryTerm subject) {
    super(type, subject);
    this.partitionId = partitionId;
  }

  /**
   * Returns the partition ID.
   *
   * @return the election partition
   */
  public PartitionId partitionId() {
    return partitionId;
  }

  /**
   * Returns the election term.
   *
   * @return the election term
   */
  public PrimaryTerm term() {
    return subject();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partition", partitionId)
        .add("term", term())
        .toString();
  }
}
