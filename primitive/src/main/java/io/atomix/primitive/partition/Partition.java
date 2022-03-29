// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.MemberId;

import java.util.Collection;

/**
 * Atomix partition.
 */
public interface Partition {

  /**
   * Returns the partition identifier.
   *
   * @return the partition identifier
   */
  PartitionId id();

  /**
   * Returns the partition term.
   *
   * @return the partition term
   */
  long term();

  /**
   * Returns the collection of all members in the partition.
   *
   * @return the collection of all members in the partition
   */
  Collection<MemberId> members();

  /**
   * Returns the partition's current primary.
   *
   * @return the partition's current primary
   */
  MemberId primary();

  /**
   * Returns the partition's backups.
   *
   * @return the partition's backups
   */
  Collection<MemberId> backups();

  /**
   * Returns the partition client.
   *
   * @return the partition client
   */
  PartitionClient getClient();

}
