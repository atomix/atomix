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

import io.atomix.utils.memory.MappedMemory;
import io.atomix.utils.memory.MappedMemoryAllocator;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * Mapped bytes.
 * <p>
 * Mapped bytes provide direct access to memory from allocated by a {@link java.nio.MappedByteBuffer}. Memory is allocated
 * by opening and expanding the given {@link File} to the desired {@code count} and mapping the file contents into memory
 * via {@link FileChannel#map(FileChannel.MapMode, long, long)}.
 * <p>
 * Closing the bytes via {@link Bytes#close()} will result in {@link Bytes#flush()}
 * being automatically called.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeMappedBytes extends NativeBytes {

  /**
   * Allocates a mapped buffer in {@link FileChannel.MapMode#READ_WRITE} mode.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link File} to the desired {@code count} and mapping the
   * file contents into memory via {@link FileChannel#map(FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param size The count of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException     If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code count} is greater than {@link MappedMemory#MAX_SIZE}
   * @see UnsafeMappedBytes#allocate(File, FileChannel.MapMode, int)
   */
  public static UnsafeMappedBytes allocate(File file, int size) {
    return allocate(file, MappedMemoryAllocator.DEFAULT_MAP_MODE, size);
  }

  /**
   * Allocates a mapped buffer.
   * <p>
   * Memory will be mapped by opening and expanding the given {@link File} to the desired {@code count} and mapping the
   * file contents into memory via {@link FileChannel#map(FileChannel.MapMode, long, long)}.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param mode The mode with which to map the file.
   * @param size The count of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   * @throws NullPointerException     If {@code file} is {@code null}
   * @throws IllegalArgumentException If {@code count} is greater than {@link MappedMemory#MAX_SIZE}
   * @see UnsafeMappedBytes#allocate(File, int)
   */
  public static UnsafeMappedBytes allocate(File file, FileChannel.MapMode mode, int size) {
    if (file == null)
      throw new NullPointerException("file cannot be null");
    if (mode == null)
      mode = MappedMemoryAllocator.DEFAULT_MAP_MODE;
    if (size > MappedMemory.MAX_SIZE)
      throw new IllegalArgumentException("size for MappedBytes cannot be greater than " + MappedMemory.MAX_SIZE);
    return new UnsafeMappedBytes(file, MappedMemory.allocate(file, mode, size));
  }

  private final File file;

  protected UnsafeMappedBytes(File file, MappedMemory memory) {
    super(memory);
    this.file = file;
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link UnsafeMappedBytes} instance backed by a copy of this instance's memory.
   */
  public UnsafeMappedBytes copy() {
    throw new UnsupportedOperationException("copy");
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public boolean isFile() {
    return true;
  }

  @Override
  public Bytes flush() {
    ((MappedMemory) memory).flush();
    return this;
  }

  @Override
  public void close() {
    ((MappedMemory) memory).close();
  }

  /**
   * Deletes the underlying file.
   */
  public void delete() {
    try {
      close();
      Files.delete(file.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
