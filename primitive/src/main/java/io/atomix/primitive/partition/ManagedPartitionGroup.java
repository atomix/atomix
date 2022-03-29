// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import java.util.concurrent.CompletableFuture;

/**
 * Managed partition group.
 */
public interface ManagedPartitionGroup extends PartitionGroup {

  /**
   * Joins the partition group.
   *
   * @param managementService the partition management service
   * @return a future to be completed once the partition group has been joined
   */
  CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService);

  /**
   * Connects to the partition group.
   *
   * @param managementService the partition management service
   * @return a future to be completed once the partition group has been connected
   */
  CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService);

  /**
   * Closes the partition group.
   *
   * @return a future to be completed once the partition group has been closed
   */
  CompletableFuture<Void> close();

}
