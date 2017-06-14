/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.memory;

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
  long address(long offset);

  /**
   * Returns the memory count.
   *
   * @return The memory count.
   */
  long size();

  /**
   * Fetches a byte from the given memory offset.
   *
   * @param offset The offset from which to fetch the byte.
   * @return The fetched byte.
   */
  byte getByte(long offset);

  /**
   * Fetches a character from the given memory offset.
   *
   * @param offset The offset from which to fetch the character.
   * @return The fetched character.
   */
  char getChar(long offset);

  /**
   * Fetches a short from the given memory offset.
   *
   * @param offset The offset from which to fetch the short.
   * @return The fetched short.
   */
  short getShort(long offset);

  /**
   * Fetches an integer from the given memory offset.
   *
   * @param offset The offset from which to fetch the integer.
   * @return The fetched integer.
   */
  int getInt(long offset);

  /**
   * Fetches a long from the given memory offset.
   *
   * @param offset The offset from which to fetch the long.
   * @return The fetched long.
   */
  long getLong(long offset);

  /**
   * Fetches a float from the given memory offset.
   *
   * @param offset The offset from which to fetch the float.
   * @return The fetched float.
   */
  float getFloat(long offset);

  /**
   * Fetches a double from the given memory offset.
   *
   * @param offset The offset from which to fetch the double.
   * @return The fetched double.
   */
  double getDouble(long offset);

  /**
   * Stores a byte at the given memory offset.
   *
   * @param offset The offset at which to store the byte.
   * @param b      The byte to store.
   */
  void putByte(long offset, byte b);

  /**
   * Stores a character at the given memory offset.
   *
   * @param offset The offset at which to store the character.
   * @param c      The character to store.
   */
  void putChar(long offset, char c);

  /**
   * Stores a short at the given memory offset.
   *
   * @param offset The offset at which to store the short.
   * @param s      The short to store.
   */
  void putShort(long offset, short s);

  /**
   * Stores an integer at the given memory offset.
   *
   * @param offset The offset at which to store the integer.
   * @param i      The integer to store.
   */
  void putInt(long offset, int i);

  /**
   * Stores a long at the given memory offset.
   *
   * @param offset The offset at which to store the long.
   * @param l      The long to store.
   */
  void putLong(long offset, long l);

  /**
   * Stores a float at the given memory offset.
   *
   * @param offset The offset at which to store the float.
   * @param f      The float to store.
   */
  void putFloat(long offset, float f);

  /**
   * Stores a double at the given memory offset.
   *
   * @param offset The offset at which to store the double.
   * @param d      The double to store.
   */
  void putDouble(long offset, double d);

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
    public static boolean isPow2(long size) {
      return size > 0 & (size & (size - 1)) == 0;
    }

    /**
     * Rounds the count to the nearest power of two.
     */
    public static long toPow2(long size) {
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
