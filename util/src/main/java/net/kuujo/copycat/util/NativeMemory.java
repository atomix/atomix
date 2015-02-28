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
package net.kuujo.copycat.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Direct memory utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeMemory {
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
   * Allocates off heap memory.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory address.
   */
  public static long allocate(long size) {
    return UNSAFE.allocateMemory(size);
  }

  /**
   * Frees off heap memory.
   *
   * @param address The address of the memory to free.
   */
  public static void free(long address) {
    UNSAFE.freeMemory(address);
  }

}
