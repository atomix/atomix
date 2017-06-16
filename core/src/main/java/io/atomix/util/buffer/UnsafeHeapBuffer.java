/*
 * Copyright 2015-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.buffer;

import io.atomix.util.concurrent.ReferenceManager;
import io.atomix.util.memory.HeapMemory;
import io.atomix.util.memory.Memory;

/**
 * Heap byte buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeHeapBuffer extends AbstractBuffer {

  /**
   * Allocates a heap buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link HeapMemory#MAX_SIZE}.
   * <p>
   * When the buffer is constructed, {@link io.atomix.util.memory.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will be initialized with a capacity of
   * {@code 4096} and have a maximum capacity of {@link HeapMemory#MAX_SIZE}. The buffer's {@code capacity} will dynamically
   * expand as bytes are written to the buffer. The underlying {@link UnsafeHeapBytes} will be initialized
   * to the next power of {@code 2}.
   *
   * @return The heap buffer.
   * @see UnsafeHeapBuffer#allocate(long)
   * @see UnsafeHeapBuffer#allocate(long, long)
   */
  public static UnsafeHeapBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, HeapMemory.MAX_SIZE);
  }

  /**
   * Allocates a heap buffer with the given initial capacity.
   * <p>
   * When the buffer is constructed, {@link io.atomix.util.memory.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will have an initial capacity of {@code capacity}.
   * The underlying {@link UnsafeHeapBytes} will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed capacity for
   *                                  an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   * @see UnsafeHeapBuffer#allocate()
   * @see UnsafeHeapBuffer#allocate(long, long)
   */
  public static UnsafeHeapBuffer allocate(long initialCapacity) {
    return allocate(initialCapacity, HeapMemory.MAX_SIZE);
  }

  /**
   * Allocates a new heap buffer.
   * <p>
   * When the buffer is constructed, {@link io.atomix.util.memory.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will have an initial capacity of
   * {@code initialCapacity} and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The
   * underlying {@link UnsafeHeapBytes} will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity     The maximum capacity of the buffer.
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code initialCapacity} or {@code maxCapacity} is greater than the
   *                                  maximum allowed count for an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   * @see UnsafeHeapBuffer#allocate()
   * @see UnsafeHeapBuffer#allocate(long)
   */
  public static UnsafeHeapBuffer allocate(long initialCapacity, long maxCapacity) {
    if (initialCapacity > maxCapacity)
      throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
    if (initialCapacity > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("initial capacity for HeapBuffer cannot be greater than " + HeapMemory.MAX_SIZE);
    if (maxCapacity > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("maximum capacity for HeapBuffer cannot be greater than " + HeapMemory.MAX_SIZE);
    return new UnsafeHeapBuffer(new UnsafeHeapBytes(HeapMemory.allocate(Memory.Util.toPow2(initialCapacity))), 0, initialCapacity, maxCapacity);
  }

  /**
   * Wraps the given bytes in a heap buffer.
   * <p>
   * The buffer will be created with an initial capacity and maximum capacity equal to the byte array count.
   *
   * @param bytes The bytes to wrap.
   * @return The wrapped bytes.
   */
  public static UnsafeHeapBuffer wrap(byte[] bytes) {
    return new UnsafeHeapBuffer(UnsafeHeapBytes.wrap(bytes), 0, bytes.length, bytes.length);
  }

  private final UnsafeHeapBytes bytes;

  protected UnsafeHeapBuffer(UnsafeHeapBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
    this.bytes = bytes;
  }

  protected UnsafeHeapBuffer(UnsafeHeapBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
    this.bytes = bytes;
  }

  @Override
  protected void compact(long from, long to, long length) {
    bytes.memory.unsafe().copyMemory(bytes.memory.array(), bytes.memory.address(from), bytes.memory.array(), bytes.memory.address(to), length);
    bytes.memory.unsafe().setMemory(bytes.memory.array(), bytes.memory.address(from), length, (byte) 0);
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public byte[] array() {
    return bytes.memory.array();
  }

  /**
   * Resets the internal heap array.
   *
   * @param array The internal array.
   * @return The heap buffer.
   */
  public UnsafeHeapBuffer reset(byte[] array) {
    bytes.memory.reset(array);
    clear();
    return this;
  }

  @Override
  public UnsafeHeapBuffer duplicate() {
    return new UnsafeHeapBuffer(bytes, offset(), capacity(), maxCapacity());
  }
}
