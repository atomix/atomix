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

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Native memory. Represents memory that can be accessed directly via {@link sun.misc.Unsafe}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeMemory implements Memory {
  public static final Unsafe UNSAFE;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError();
    }
  }

  /**
   * Allocates native memory.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory.
   */
  public static Memory allocate(long size) {
    return new NativeAllocator().allocate(size);
  }

  private long address;
  private long size;
  private final Allocator allocator;

  public NativeMemory(long address, long size, Allocator allocator) {
    if (allocator == null)
      throw new NullPointerException("allocator cannot be null");
    this.address = address;
    this.size = size;
    this.allocator = allocator;
  }

  @Override
  public long address() {
    return address;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public void clear() {
    UNSAFE.setMemory(address, size, (byte) 0);
  }

  @Override
  public void free() {
    if (address != 0) {
      allocator.free(this);
      address = 0;
    }
  }

}
