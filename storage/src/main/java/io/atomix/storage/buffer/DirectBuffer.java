// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import io.atomix.utils.memory.Memory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBuffer extends ByteBufferBuffer {

  /**
   * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link Long#MAX_VALUE}.
   *
   * @return The direct buffer.
   * @see DirectBuffer#allocate(int)
   * @see DirectBuffer#allocate(int, int)
   */
  public static DirectBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, MAX_SIZE);
  }

  /**
   * Allocates a direct buffer with the given initial capacity.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed count for
   *                                  a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   * @see DirectBuffer#allocate()
   * @see DirectBuffer#allocate(int, int)
   */
  public static DirectBuffer allocate(int initialCapacity) {
    return allocate(initialCapacity, MAX_SIZE);
  }

  /**
   * Allocates a new direct buffer.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity     The maximum capacity of the buffer.
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} or {@code maxCapacity} is greater than the maximum
   *                                  allowed count for a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   * @see DirectBuffer#allocate()
   * @see DirectBuffer#allocate(int)
   */
  public static DirectBuffer allocate(int initialCapacity, int maxCapacity) {
    checkArgument(initialCapacity <= maxCapacity, "initial capacity cannot be greater than maximum capacity");
    return new DirectBuffer(DirectBytes.allocate((int) Math.min(Memory.Util.toPow2(initialCapacity), MAX_SIZE)), 0, initialCapacity, maxCapacity);
  }

  protected DirectBuffer(DirectBytes bytes, int offset, int initialCapacity, int maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
  }

  @Override
  public DirectBuffer duplicate() {
    return new DirectBuffer((DirectBytes) bytes, offset(), capacity(), maxCapacity());
  }
}
