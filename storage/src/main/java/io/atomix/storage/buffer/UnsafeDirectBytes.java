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

import io.atomix.utils.memory.DirectMemory;

/**
 * Direct byte buffer bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeDirectBytes extends NativeBytes {

  /**
   * Allocates a direct {@link java.nio.ByteBuffer} based byte array.
   * <p>
   * When the array is constructed, {@link io.atomix.utils.memory.DirectMemoryAllocator} will be used to allocate
   * {@code count} bytes of off-heap memory. Memory is accessed by the buffer directly via {@link sun.misc.Unsafe}.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The native buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *                                  a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   */
  public static UnsafeDirectBytes allocate(int size) {
    return new UnsafeDirectBytes(DirectMemory.allocate(size));
  }

  protected UnsafeDirectBytes(DirectMemory memory) {
    super(memory);
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link UnsafeHeapBytes} instance backed by a copy of this instance's array.
   */
  public UnsafeDirectBytes copy() {
    return new UnsafeDirectBytes((DirectMemory) memory.copy());
  }

}
