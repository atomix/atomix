/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.storage.buffer;

import io.atomix.utils.concurrent.ReferenceManager;
import io.atomix.utils.memory.DirectMemory;
import io.atomix.utils.memory.Memory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeDirectBuffer extends NativeBuffer {

  /**
   * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link Long#MAX_VALUE}.
   * <p>
   * When the buffer is constructed, {@link io.atomix.utils.memory.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will be initialized with a capacity of {@code 4096}
   * and have a maximum capacity of {@link Long#MAX_VALUE}. The buffer's {@code capacity} will dynamically expand as
   * bytes are written to the buffer. The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
   *
   * @return The direct buffer.
   * @see UnsafeDirectBuffer#allocate(int)
   * @see UnsafeDirectBuffer#allocate(int, int)
   */
  public static UnsafeDirectBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, Integer.MAX_VALUE);
  }

  /**
   * Allocates a direct buffer with the given initial capacity.
   * <p>
   * When the buffer is constructed, {@link io.atomix.utils.memory.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code capacity}.
   * The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed count for
   *                                  a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   * @see UnsafeDirectBuffer#allocate()
   * @see UnsafeDirectBuffer#allocate(int, int)
   */
  public static UnsafeDirectBuffer allocate(int initialCapacity) {
    return allocate(initialCapacity, Integer.MAX_VALUE);
  }

  /**
   * Allocates a new direct buffer.
   * <p>
   * When the buffer is constructed, {@link io.atomix.utils.memory.DirectMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code initialCapacity}
   * and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The underlying {@link UnsafeDirectBytes}
   * will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity     The maximum capacity of the buffer.
   * @return The direct buffer.
   * @throws IllegalArgumentException If {@code capacity} or {@code maxCapacity} is greater than the maximum
   *                                  allowed count for a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   * @see UnsafeDirectBuffer#allocate()
   * @see UnsafeDirectBuffer#allocate(int)
   */
  public static UnsafeDirectBuffer allocate(int initialCapacity, int maxCapacity) {
    checkArgument(initialCapacity <= maxCapacity, "initial capacity cannot be greater than maximum capacity");
    return new UnsafeDirectBuffer(new UnsafeDirectBytes(DirectMemory.allocate((int) Math.min(Memory.Util.toPow2(initialCapacity), maxCapacity))), 0, initialCapacity, maxCapacity);
  }

  protected UnsafeDirectBuffer(UnsafeDirectBytes bytes, int offset, int initialCapacity, int maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity);
  }

  protected UnsafeDirectBuffer(UnsafeDirectBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
  }

  @Override
  public UnsafeDirectBuffer duplicate() {
    return new UnsafeDirectBuffer((UnsafeDirectBytes) bytes, offset(), capacity(), maxCapacity());
  }
}
