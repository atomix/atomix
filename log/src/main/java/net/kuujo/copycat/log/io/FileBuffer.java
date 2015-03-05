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
package net.kuujo.copycat.log.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * Direct file buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBuffer extends CheckedBuffer {
  private static final String DEFAULT_MODE = "rw";

  /**
   * Allocates a file buffer of unlimited size.
   * <p>
   * The buffer will be allocated with {@link Long#MAX_VALUE} bytes. As bytes are written to the buffer, the underlying
   * {@link java.io.RandomAccessFile} will expand.
   *
   * @param file The file to allocate.
   * @return The allocated buffer.
   */
  public static Buffer allocate(File file) {
    return allocate(file, DEFAULT_MODE, Long.MAX_VALUE);
  }

  /**
   * Allocates a file buffer.
   * <p>
   * If the underlying file is empty, the file size will expand dynamically as bytes are written to the file.
   *
   * @param file The file to allocate.
   * @param size The size of the bytes to allocate.
   * @return The allocated buffer.
   */
  public static Buffer allocate(File file, long size) {
    return allocate(file, DEFAULT_MODE, size);
  }

  /**
   * Allocates a file buffer.
   * <p>
   * If the underlying file is empty, the file size will expand dynamically as bytes are written to the file.
   *
   * @param file The file to allocate.
   * @param mode The mode in which to open the underlying {@link java.io.RandomAccessFile}.
   * @param size The size of the bytes to allocate.
   * @return The allocated buffer.
   */
  public static Buffer allocate(File file, String mode, long size) {
    try {
      return new FileBuffer(new FileBytes(new RandomAccessFile(file, mode), 0, size));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private FileBuffer(Bytes bytes) {
    super(bytes);
  }

}
