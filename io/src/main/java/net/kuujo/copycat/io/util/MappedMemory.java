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
package net.kuujo.copycat.io.util;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mapped memory.
 * <p>
 * This is a special memory descriptor that handles management of {@link java.nio.MappedByteBuffer} based memory. The
 * mapped memory descriptor simply points to the memory address of the underlying byte buffer. When memory is reallocated,
 * the parent {@link MappedMemoryAllocator} is used to create a new {@link java.nio.MappedByteBuffer}
 * and free the existing buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedMemory extends NativeMemory {
  public static final long MAX_SIZE = Integer.MAX_VALUE;

  /**
   * Allocates memory mapped to a file on disk.
   *
   * @param file The file to which to map memory.
   * @param size The size of the memory to map.
   * @return The mapped memory.
   * @throws IllegalArgumentException If {@code size} is greater than {@link Integer#MAX_VALUE}
   */
  public static MappedMemory allocate(File file, long size) {
    return new MappedMemoryAllocator(file).allocate(size);
  }

  /**
   * Allocates memory mapped to a file on disk.
   *
   * @param file The file to which to map memory.
   * @param mode The mode with which to map memory.
   * @param size The size of the memory to map.
   * @return The mapped memory.
   * @throws IllegalArgumentException If {@code size} is greater than {@link Integer#MAX_VALUE}
   */
  public static MappedMemory allocate(File file, FileChannel.MapMode mode, long size) {
    if (size > MAX_SIZE)
      throw new IllegalArgumentException("size cannot be greater than " + MAX_SIZE);
    return new MappedMemoryAllocator(file, mode).allocate(size);
  }

  private final MappedByteBuffer buffer;

  public MappedMemory(MappedByteBuffer buffer, MappedMemoryAllocator allocator) {
    super(((DirectBuffer) buffer).address(), buffer.capacity(), allocator);
    this.buffer = buffer;
  }

  /**
   * Flushes the mapped buffer to disk.
   */
  public void flush() {
    buffer.force();
  }

  @Override
  public void free() {
    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
    if (cleaner != null)
      cleaner.clean();
    ((MappedMemoryAllocator) allocator).release();
  }

}
