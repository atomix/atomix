// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

/**
 * Buffer allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface BufferAllocator {

  /**
   * Allocates a dynamic capacity buffer.
   *
   * @return The allocated buffer.
   */
  Buffer allocate();

  /**
   * Allocates a dynamic capacity buffer with the given initial capacity.
   *
   * @param initialCapacity The initial buffer capacity.
   * @return The allocated buffer.
   */
  Buffer allocate(int initialCapacity);

  /**
   * Allocates a new buffer.
   *
   * @param initialCapacity The initial buffer capacity.
   * @param maxCapacity     The maximum buffer capacity.
   * @return The allocated buffer.
   */
  Buffer allocate(int initialCapacity, int maxCapacity);

}
