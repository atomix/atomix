// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.ListenerService;

/**
 * Partition primary election service.
 * <p>
 * The primary election service is used to elect primaries and backups for primary-backup replication protocols.
 * Each partition is provided a distinct {@link PrimaryElection} through which it elects a primary.
 */
public interface PrimaryElectionService extends ListenerService<PrimaryElectionEvent, PrimaryElectionEventListener> {

  /**
   * Returns the primary election for the given partition identifier.
   *
   * @param partitionId the partition identifier for which to return the primary election
   * @return the primary election for the given partition identifier
   */
  PrimaryElection getElectionFor(PartitionId partitionId);

}
