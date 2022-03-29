// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.memory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Memory size.
 */
public class MemorySize {

  /**
   * Creates a memory size from the given bytes.
   *
   * @param bytes the number of bytes
   * @return the memory size
   */
  public static MemorySize from(long bytes) {
    return new MemorySize(bytes);
  }

  private final long bytes;

  public MemorySize(long bytes) {
    this.bytes = bytes;
  }

  /**
   * Returns the number of bytes.
   *
   * @return the number of bytes
   */
  public long bytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(bytes).hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof MemorySize && ((MemorySize) object).bytes == bytes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(bytes)
        .toString();
  }
}
