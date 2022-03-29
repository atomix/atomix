// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.roles;

import io.atomix.protocols.backup.protocol.BackupOperation;

import java.util.concurrent.CompletableFuture;

/**
 * Backup replicator.
 */
interface Replicator {

  /**
   * Backs up the given operation.
   *
   * @param operation the operation to back up
   * @return a future to be completed with the operation index
   */
  CompletableFuture<Void> replicate(BackupOperation operation);

  /**
   * Closes the replicator.
   */
  void close();
}
