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

import net.kuujo.copycat.io.util.MappedMemory;
import net.kuujo.copycat.io.util.MappedMemoryAllocator;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * Mapped bytes.
 * <p>
 * Mapped bytes provide direct access to memory from allocated by a {@link java.nio.MappedByteBuffer}. Memory is allocated
 * by opening and expanding the given {@link java.io.File} to the desired {@code size} and mapping the file contents into memory
 * via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
 * <p>
 * Closing the bytes via {@link net.kuujo.copycat.io.Bytes#close()} will result in {@link net.kuujo.copycat.io.Bytes#flush()}
 * being automatically called.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedBytes extends NativeBytes {

  /**
   * Allocates a mapped buffer in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code size} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param size The size of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code size} is greater than {@link MappedMemory#MAX_SIZE}
   *
   * @see MappedBytes#allocate(java.io.File, java.nio.channels.FileChannel.MapMode, long)
   */
  public static MappedBytes allocate(File file, long size) {
    return allocate(file, MappedMemoryAllocator.DEFAULT_MAP_MODE, size);
  }

  /**
   * Allocates a mapped buffer.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link java.io.File} to the desired {@code size} and mapping the
   * file contents into memory via {@link java.nio.channels.FileChannel#map(java.nio.channels.FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param mode The mode with which to map the file.
   * @param size The size of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code size} is greater than {@link MappedMemory#MAX_SIZE}
   *
   * @see MappedBytes#allocate(java.io.File, long)
   */
  public static MappedBytes allocate(File file, FileChannel.MapMode mode, long size) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = MappedMemoryAllocator.DEFAULT_MAP_MODE;
    if (size > MappedMemory.MAX_SIZE)
      throw new IllegalArgumentException("size for MappedBytes cannot be greater than " + MappedMemory.MAX_SIZE);
    return new MappedBytes(MappedMemory.allocate(file, mode, size));
  }

  protected MappedBytes(MappedMemory memory) {
    super(memory);
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link net.kuujo.copycat.io.MappedBytes} instance backed by a copy of this instance's memory.
   */
  public MappedBytes copy() {
    throw new UnsupportedOperationException("copy");
  }

  @Override
  public Bytes flush() {
    ((MappedMemory) memory).flush();
    return this;
  }

}
