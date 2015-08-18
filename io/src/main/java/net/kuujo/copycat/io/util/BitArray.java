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

import net.kuujo.copycat.io.HeapBytes;

import java.io.IOException;

/**
 * Direct memory bit set.
 * <p>
 * The direct bit set performs bitwise operations on a fixed count {@link net.kuujo.copycat.io.HeapBytes} instance.
 * Currently, all bytes are {@link net.kuujo.copycat.io.HeapBytes}, but theoretically {@link net.kuujo.copycat.io.MappedBytes}
 * could be used for durability as well.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BitArray implements AutoCloseable {

  /**
   * Allocates a new direct bit set.
   *
   * @param bits The number of bits in the bit set.
   * @return The allocated bit set.
   */
  public static BitArray allocate(long bits) {
    if (!(bits > 0 & (bits & (bits - 1)) == 0))
      throw new IllegalArgumentException("size must be a power of 2");
    return new BitArray(HeapBytes.allocate(Math.max(bits / 8 + 8, 8)), bits);
  }

  private final HeapBytes bytes;
  private long size;
  private long count;

  private BitArray(HeapBytes bytes, long size) {
    this(bytes, 0, size);
  }

  private BitArray(HeapBytes bytes, long count, long size) {
    this.bytes = bytes;
    this.size = size;
    this.count = count;
  }

  /**
   * Returns the offset of the long that stores the bit for the given index.
   */
  private long offset(long index) {
    return (index / 64) * 8;
  }

  /**
   * Returns the position of the bit for the given index.
   */
  private long position(long index) {
    return index % 64;
  }

  /**
   * Sets the bit at the given index.
   *
   * @param index The index of the bit to set.
   * @return Indicates if the bit was changed.
   */
  public boolean set(long index) {
    if (!(index < size))
      throw new IndexOutOfBoundsException();
    if (!get(index)) {
      bytes.writeLong(offset(index), bytes.readLong(offset(index)) | (1l << position(index)));
      count++;
      return true;
    }
    return false;
  }

  /**
   * Gets the bit at the given index.
   *
   * @param index The index of the bit to get.
   * @return Indicates whether the bit is set.
   */
  public boolean get(long index) {
    if (!(index < size))
      throw new IndexOutOfBoundsException();
    return (bytes.readLong(offset(index)) & (1l << (position(index)))) != 0;
  }

  /**
   * Returns the total number of bits in the bit set.
   *
   * @return The total number of bits in the bit set.
   */
  public long size() {
    return size;
  }

  /**
   * Returns the number of bits set in the bit set.
   *
   * @return The number of bits set.
   */
  public long count() {
    return count;
  }

  /**
   * Resizes the bit array to a new count.
   *
   * @param size The new count.
   * @return The resized bit array.
   */
  public BitArray resize(long size) {
    bytes.resize(Math.max(size / 64, 8) + 8);
    this.size = size;
    return this;
  }

  /**
   * Copies the bit set to a new memory address.
   *
   * @return The copied bit set.
   */
  public BitArray copy() {
    return new BitArray(bytes.copy(), count);
  }

  @Override
  public void close() throws IOException {
    bytes.close();
  }

}
