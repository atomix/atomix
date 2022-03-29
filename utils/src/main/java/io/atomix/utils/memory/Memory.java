// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.memory;

/**
 * Memory allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Memory {

  /**
   * Returns the memory count.
   *
   * @return The memory count.
   */
  int size();

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
      if ((size & (size - 1)) == 0) {
        return size;
      }
      int i = 128;
      while (i < size) {
        i *= 2;
        if (i <= 0) {
          return 1L << 62;
        }
      }
      return i;
    }
  }

}
