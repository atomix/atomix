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

import net.kuujo.copycat.io.util.Memory;
import net.kuujo.copycat.io.util.NativeMemory;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Native byte buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeBuffer extends AbstractBuffer {

  /**
   * Allocates an unlimited capacity native buffer with an initial capacity of {@code 4096}.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.NativeMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will be initialized with a capacity of {@code 4096}
   * and have a maximum capacity of {@link Long#MAX_VALUE}. The buffer's {@code capacity} will dynamically expand as
   * bytes are written to the buffer. The underlying {@link NativeBytes} will be initialized to the next power of {@code 2}.
   *
   * @return The native buffer.
   *
   * @see NativeBuffer#allocate(long)
   * @see NativeBuffer#allocate(long, long)
   */
  public static NativeBuffer allocate() {
    return allocate(DEFAULT_INITIAL_CAPACITY, Long.MAX_VALUE);
  }

  /**
   * Allocates a fixed capacity native buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.NativeMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have a fixed capacity of {@code capacity}.
   * The underlying {@link NativeBytes} will be initialized to the next power of {@code 2}.
   *
   * @param capacity The capacity of the buffer to allocate (in bytes).
   * @return The native buffer.
   *
   * @see NativeBuffer#allocate()
   * @see NativeBuffer#allocate(long, long)
   */
  public static NativeBuffer allocate(long capacity) {
    return allocate(capacity, capacity);
  }

  /**
   * Allocates a new native buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.NativeMemoryAllocator} will be used to allocate
   * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code initialCapacity}
   * and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The underlying {@link NativeBytes}
   * will be initialized to the next power of {@code 2}.
   *
   * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
   * @param maxCapacity The maximum capacity of the buffer.
   * @return The native buffer.
   * @throws IllegalArgumentException If {@code initialCapacity} is greater than {@code maxCapacity}
   *
   * @see NativeBuffer#allocate()
   * @see NativeBuffer#allocate(long)
   */
  public static NativeBuffer allocate(long initialCapacity, long maxCapacity) {
    if (initialCapacity > maxCapacity)
      throw new IllegalArgumentException("initial capacity cannot be greater than maximum capacity");
    return new NativeBuffer(new NativeBytes(NativeMemory.allocate(Memory.toPow2(initialCapacity))), 0, initialCapacity, maxCapacity);
  }

  protected NativeBuffer(NativeBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
  }

  protected NativeBuffer(NativeBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
  }

  @Override
  protected Buffer createView(ReferenceManager<Buffer> referenceManager) {
    return new NativeBuffer((NativeBytes) bytes, referenceManager);
  }

}
