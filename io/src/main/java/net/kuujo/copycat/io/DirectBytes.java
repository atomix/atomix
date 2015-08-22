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

/**
 * Direct byte buffer bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBytes extends NativeBytes {

  /**
   * Allocates a direct {@link java.nio.ByteBuffer} based byte array.
   * <p>
   * When the array is constructed, {@link net.kuujo.copycat.io.util.DirectMemoryAllocator} will be used to allocate
   * {@code count} bytes of off-heap memory. Memory is accessed by the buffer directly via {@link sun.misc.Unsafe}.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The native buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *         a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
   */
  public static DirectBytes allocate(long size) {
    return new DirectBytes(DirectMemory.allocate(size));
  }

  protected DirectBytes(DirectMemory memory) {
    super(memory);
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link HeapBytes} instance backed by a copy of this instance's array.
   */
  public DirectBytes copy() {
    return new DirectBytes((DirectMemory) memory.copy());
  }

}
