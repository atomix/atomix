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

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Memory mapped file buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedBuffer extends CheckedBuffer {
  private static final FileChannel.MapMode DEFAULT_MODE = FileChannel.MapMode.READ_WRITE;

  /**
   * Allocates a mapped buffer in {@link FileChannel.MapMode#READ_WRITE} mode.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param size The size of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   */
  public static Buffer allocate(File file, long size) {
    return allocate(file, DEFAULT_MODE, size);
  }

  /**
   * Allocates a mapped buffer.
   *
   * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
   * @param mode The mode with which to map the file.
   * @param size The size of the buffer to allocate (in bytes).
   * @return The mapped buffer.
   */
  public static Buffer allocate(File file, FileChannel.MapMode mode, long size) {
    try {
      FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
      MappedByteBuffer buffer = channel.map(mode, 0, size);
      return new MappedBuffer(new NativeBytes(new MappedMemory(buffer)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final NativeBytes bytes;

  private MappedBuffer(NativeBytes bytes) {
    super(bytes);
    this.bytes = bytes;
  }

  private MappedBuffer(NativeBytes bytes, long offset) {
    super(bytes, offset);
    this.bytes = bytes;
  }

  private MappedBuffer(NativeBytes bytes, long offset, long length) {
    super(bytes, offset, length);
    this.bytes = bytes;
  }

  @Override
  public Buffer slice() {
    return new MappedBuffer(bytes, position());
  }

  @Override
  public Buffer slice(long offset, long length) {
    return new MappedBuffer(bytes, offset, length);
  }

  @Override
  public void close() throws Exception {
    bytes.memory().free();
  }

}
