// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage;

/**
 * Storage level configuration values which control how logs are stored on disk or in memory.
 */
public enum StorageLevel {

  /**
   * Stores data in memory only.
   */
  @Deprecated
  MEMORY,

  /**
   * Stores data in a memory-mapped file.
   */
  MAPPED,

  /**
   * Stores data on disk.
   */
  DISK

}
