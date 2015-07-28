/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.io;

import net.kuujo.copycat.io.util.HeapMemory;
import net.kuujo.copycat.io.util.Memory;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Heap byte buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapBuffer extends AbstractBuffer {

  /**
   * Allocates a heap buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link HeapMemory#MAX_SIZE}.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will be initialized with a capacity of
   * {@code 4096} and have a maximum capacity of {@link HeapMemory#MAX_SIZE}. The buffer's {@code capacity} will dynamically
   * expand as bytes are written to the buffer. The underlying {@link HeapBytes} will be initialized
   * to the next power of {@code 2}.
   *
   * @return The heap buffer.
   *
   * @see HeapBuffer#allocate(long)
   * @see HeapBuffer#allocate(long, long)
   */
  public static HeapBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, HeapMemory.MAX_SIZE);
  }

  /**
   * Allocates a fixed capacity heap buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will have a fixed capacity of {@code capacity}.
   * The underlying {@link HeapBytes} will be initialized to the next power of {@code 2}.
   *
   * @param capacity The capacity of the buffer to allocate (in bytes).
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed capacity for
   *         an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   *
   * @see HeapBuffer#allocate()
   * @see HeapBuffer#allocate(long, long)
   */
  public static HeapBuffer allocate(long capacity) {
    return allocate(capacity, capacity);
  }

  /**
   * Allocates a new heap buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.HeapMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of memory on the Java heap. The resulting buffer will have an initial capacity of
   * {@code initialCapacity} and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The
   * underlying {@link HeapBytes} will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity The maximum capacity of the buffer.
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code initialCapacity} or {@code maxCapacity} is greater than the
   *         maximum allowed size for an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   *
   * @see HeapBuffer#allocate()
   * @see HeapBuffer#allocate(long)
   */
  public static HeapBuffer allocate(long initialCapacity, long maxCapacity) {
    if (initialCapacity > maxCapacity)
      throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
    if (initialCapacity > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("initial capacity for HeapBuffer cannot be greater than " + HeapMemory.MAX_SIZE);
    if (maxCapacity > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("maximum capacity for HeapBuffer cannot be greater than " + HeapMemory.MAX_SIZE);
    return new HeapBuffer(new HeapBytes(HeapMemory.allocate(Memory.Util.toPow2(initialCapacity))), 0, initialCapacity, maxCapacity);
  }

  /**
   * Wraps the given bytes in a heap buffer.
   * <p>
   * The buffer will be created with an initial capacity and maximum capacity equal to the byte array size.
   *
   * @param bytes The bytes to wrap.
   * @return The wrapped bytes.
   */
  public static HeapBuffer wrap(byte[] bytes) {
    return new HeapBuffer(HeapBytes.wrap(bytes), 0, bytes.length, bytes.length);
  }

  private final HeapBytes bytes;

  protected HeapBuffer(HeapBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
    this.bytes = bytes;
  }

  protected HeapBuffer(HeapBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
    this.bytes = bytes;
  }

  @Override
  protected void compact(long from, long to, long length) {
    bytes.memory.unsafe().copyMemory(bytes.memory.array(), bytes.memory.address(from), bytes.memory.array(), bytes.memory.address(to), length);
    bytes.memory.unsafe().setMemory(bytes.memory.array(), bytes.memory.address(from), length, (byte) 0);
  }

  /**
   * Returns the underlying byte array.
   *
   * @return The underlying byte array.
   */
  public byte[] array() {
    return bytes.memory.array();
  }

  /**
   * Resets the internal heap array.
   *
   * @param array The internal array.
   * @return The heap buffer.
   */
  public HeapBuffer reset(byte[] array) {
    bytes.memory.reset(array);
    clear();
    return this;
  }

}
