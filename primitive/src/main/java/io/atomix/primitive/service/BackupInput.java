// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.service;

import io.atomix.storage.buffer.BufferInput;

/**
 * Backup input.
 */
public interface BackupInput extends BufferInput<BackupInput> {

  /**
   * Deserializes an object from the input.
   *
   * @param <U> the object type
   * @return the object to deserialize
   */
  <U> U readObject();

}
