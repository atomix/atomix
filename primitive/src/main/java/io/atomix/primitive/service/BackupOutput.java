// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.service;

import io.atomix.storage.buffer.BufferOutput;

/**
 * Backup output.
 */
public interface BackupOutput extends BufferOutput<BackupOutput> {

  /**
   * Serializes an object to the output.
   *
   * @param object the object to serialize
   * @param <U> the object type
   * @return the backup output
   */
  <U> BackupOutput writeObject(U object);

}
