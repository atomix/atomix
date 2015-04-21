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

import net.kuujo.copycat.io.util.DirectMemory;
import net.kuujo.copycat.io.util.Memory;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBuffer extends NativeBuffer {

  /**
   * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link DirectMemory#MAX_SIZE}.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will be initialized with a capacity of {@code 4096}
   * and have a maximum capacity of {@link DirectMemory#MAX_SIZE}. The buffer's {@code capacity} will dynamically expand as
   * bytes are written to the buffer. The underlying {@link DirectBytes} will be initialized to the next power of {@code 2}.
   *
   * @return The direct buffer.
   *
   * @see DirectBuffer#allocate(long)
   * @see DirectBuffer#allocate(long, long)
   */
  public static DirectBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, DirectMemory.MAX_SIZE);
  }

  /**
   * Allocates a fixed capacity direct buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have a fixed capacity of {@code capacity}.
   * The underlying {@link DirectBytes} will be initialized to the next power of {@code 2}.
   *
   * @param capacity The capacity of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed size for
   *         a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   *
   * @see DirectBuffer#allocate()
   * @see DirectBuffer#allocate(long, long)
   */
  public static DirectBuffer allocate(long capacity) {
    return allocate(capacity, capacity);
  }

  /**
   * Allocates a new direct buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code initialCapacity}
   * and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The underlying {@link DirectBytes}
   * will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity The maximum capacity of the buffer.
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} or {@code maxCapacity} is greater than the maximum
   *         allowed size for a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   *
   * @see DirectBuffer#allocate()
   * @see DirectBuffer#allocate(long)
   */
  public static DirectBuffer allocate(long initialCapacity, long maxCapacity) {
    if (initialCapacity > maxCapacity)
      throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
    if (initialCapacity > DirectMemory.MAX_SIZE)
      throw new IllegalArgumentException("initial capacity for DirectBuffer cannot be greater than " + DirectMemory.MAX_SIZE);
    if (maxCapacity > DirectMemory.MAX_SIZE)
      throw new IllegalArgumentException("maximum capacity for DirectBuffer cannot be greater than " + DirectMemory.MAX_SIZE);
    return new DirectBuffer(new DirectBytes(DirectMemory.allocate(Memory.toPow2(initialCapacity))), 0, initialCapacity, maxCapacity);
  }

  protected DirectBuffer(DirectBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity);
  }

  protected DirectBuffer(DirectBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
  }

  @Override
  protected Buffer createView(ReferenceManager<Buffer> referenceManager) {
    return new DirectBuffer((DirectBytes) bytes, referenceManager);
  }

}
