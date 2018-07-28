/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.utils.memory;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mapped memory.
 * <p>
 * This is a special memory descriptor that handles management of {@link MappedByteBuffer} based memory. The
 * mapped memory descriptor simply points to the memory address of the underlying byte buffer. When memory is reallocated,
 * the parent {@link MappedMemoryAllocator} is used to create a new {@link MappedByteBuffer}
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
   * @param size The count of the memory to map.
   * @return The mapped memory.
   * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
   */
  public static MappedMemory allocate(File file, int size) {
    return new MappedMemoryAllocator(file).allocate(size);
  }

  /**
   * Allocates memory mapped to a file on disk.
   *
   * @param file The file to which to map memory.
   * @param mode The mode with which to map memory.
   * @param size The count of the memory to map.
   * @return The mapped memory.
   * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
   */
  public static MappedMemory allocate(File file, FileChannel.MapMode mode, int size) {
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
    Util.CLEANER.freeDirectBuffer(buffer);
    ((MappedMemoryAllocator) allocator).release();
  }

  public void close() {
    free();
    ((MappedMemoryAllocator) allocator).close();
  }

  /*
   * Copyright 2013 The Netty Project
   *
   * The Netty Project licenses this file to you under the Apache License,
   * version 2.0 (the "License"); you may not use this file except in compliance
   * with the License. You may obtain a copy of the License at:
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   * License for the specific language governing permissions and limitations
   * under the License.
   */
  public static class Util {

    public static final Cleaner CLEANER;

    private static final Cleaner NOOP = buffer -> {
      // NOOP
    };

    static {
      if (majorVersionFromJavaSpecificationVersion() >= 9) {
        CLEANER = CleanerJava9.isSupported() ? new CleanerJava9() : NOOP;
      } else {
        CLEANER = CleanerJava8.isSupported() ? new CleanerJava8() : NOOP;
      }
    }

    private static int majorVersionFromJavaSpecificationVersion() {
      return majorVersion(System.getProperty("java.specification.version", "1.8"));
    }

    private static int majorVersion(final String javaSpecVersion) {
      final String[] components = javaSpecVersion.split("\\.");
      final int[] version = new int[components.length];
      for (int i = 0; i < components.length; i++) {
        version[i] = Integer.parseInt(components[i]);
      }

      if (version[0] == 1) {
        assert version[1] >= 8;
        return version[1];
      } else {
        return version[0];
      }
    }
  }
}
