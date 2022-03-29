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
public class HeapBuffer extends ByteBufferBuffer {

  /**
   * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link Long#MAX_VALUE}.
   *
   * @return The direct buffer.
   * @see HeapBuffer#allocate(int)
   * @see HeapBuffer#allocate(int, int)
   */
  public static HeapBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, MAX_SIZE);
  }

  /**
   * Allocates a direct buffer with the given initial capacity.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed count for
   *                                  a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   * @see HeapBuffer#allocate()
   * @see HeapBuffer#allocate(int, int)
   */
  public static HeapBuffer allocate(int initialCapacity) {
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
   * @see HeapBuffer#allocate()
   * @see HeapBuffer#allocate(int)
   */
  public static HeapBuffer allocate(int initialCapacity, int maxCapacity) {
    checkArgument(initialCapacity <= maxCapacity, "initial capacity cannot be greater than maximum capacity");
    return new HeapBuffer(HeapBytes.allocate((int) Math.min(Memory.Util.toPow2(initialCapacity), MAX_SIZE)), 0, initialCapacity, maxCapacity);
  }

  /**
   * Wraps the given bytes in a heap buffer.
   * <p>
   * The buffer will be created with an initial capacity and maximum capacity equal to the byte array count.
   *
   * @param bytes The bytes to wrap.
   * @return The wrapped bytes.
   */
  public static HeapBuffer wrap(byte[] bytes) {
    return new HeapBuffer(HeapBytes.wrap(bytes), 0, bytes.length, bytes.length);
  }

  private final HeapBytes bytes;

  protected HeapBuffer(HeapBytes bytes, int offset, int initialCapacity, int maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
    this.bytes = bytes;
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public HeapBuffer duplicate() {
    return new HeapBuffer(bytes, offset(), capacity(), maxCapacity());
  }
}
