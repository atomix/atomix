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

/**
 * Memory allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Memory {

  /**
   * Returns the memory allocator that allocated this memory.
   *
   * @return The memory allocator.
   */
  MemoryAllocator allocator();

  /**
   * Returns the memory address.
   *
   * @return The memory address.
   */
  long address();

  /**
   * Returns the memory address for the given offset.
   *
   * @param offset The offset for which to return the address.
   * @return The memory address for the given offset.
   */
  long address(int offset);

  /**
   * Returns the memory count.
   *
   * @return The memory count.
   */
  int size();

  /**
   * Fetches a byte from the given memory offset.
   *
   * @param offset The offset from which to fetch the byte.
   * @return The fetched byte.
   */
  byte getByte(int offset);

  /**
   * Fetches a character from the given memory offset.
   *
   * @param offset The offset from which to fetch the character.
   * @return The fetched character.
   */
  char getChar(int offset);

  /**
   * Fetches a short from the given memory offset.
   *
   * @param offset The offset from which to fetch the short.
   * @return The fetched short.
   */
  short getShort(int offset);

  /**
   * Fetches an integer from the given memory offset.
   *
   * @param offset The offset from which to fetch the integer.
   * @return The fetched integer.
   */
  int getInt(int offset);

  /**
   * Fetches a long from the given memory offset.
   *
   * @param offset The offset from which to fetch the long.
   * @return The fetched long.
   */
  long getLong(int offset);

  /**
   * Fetches a float from the given memory offset.
   *
   * @param offset The offset from which to fetch the float.
   * @return The fetched float.
   */
  float getFloat(int offset);

  /**
   * Fetches a double from the given memory offset.
   *
   * @param offset The offset from which to fetch the double.
   * @return The fetched double.
   */
  double getDouble(int offset);

  /**
   * Stores a byte at the given memory offset.
   *
   * @param offset The offset at which to store the byte.
   * @param b      The byte to store.
   */
  void putByte(int offset, byte b);

  /**
   * Stores a character at the given memory offset.
   *
   * @param offset The offset at which to store the character.
   * @param c      The character to store.
   */
  void putChar(int offset, char c);

  /**
   * Stores a short at the given memory offset.
   *
   * @param offset The offset at which to store the short.
   * @param s      The short to store.
   */
  void putShort(int offset, short s);

  /**
   * Stores an integer at the given memory offset.
   *
   * @param offset The offset at which to store the integer.
   * @param i      The integer to store.
   */
  void putInt(int offset, int i);

  /**
   * Stores a long at the given memory offset.
   *
   * @param offset The offset at which to store the long.
   * @param l      The long to store.
   */
  void putLong(int offset, long l);

  /**
   * Stores a float at the given memory offset.
   *
   * @param offset The offset at which to store the float.
   * @param f      The float to store.
   */
  void putFloat(int offset, float f);

  /**
   * Stores a double at the given memory offset.
   *
   * @param offset The offset at which to store the double.
   * @param d      The double to store.
   */
  void putDouble(int offset, double d);

  /**
   * Copies the memory to a distinct memory address.
   *
   * @return The copied memory descriptor.
   */
  Memory copy();

  /**
   * Clears the memory.
   */
  void clear();

  /**
   * Frees the memory.
   */
  void free();

  /**
   * Memory utilities.
   */
  class Util {

    /**
     * Returns a boolean indicating whether the given count is a power of 2.
     */
    public static boolean isPow2(int size) {
      return size > 0 & (size & (size - 1)) == 0;
    }

    /**
     * Rounds the count to the nearest power of two.
     */
    public static long toPow2(int size) {
      if ((size & (size - 1)) == 0)
        return size;
      int i = 128;
      while (i < size) {
        i *= 2;
        if (i <= 0) return 1L << 62;
      }
      return i;
    }
  }

}
