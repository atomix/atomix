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

import net.kuujo.copycat.io.NativeBytes;

import java.io.IOException;

/**
 * Direct memory bit set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBitSet implements AutoCloseable {
  private static final NativeAllocator allocator = new NativeAllocator();

  /**
   * Allocates a new direct bit set.
   *
   * @param bits The number of bits in the bit set.
   * @return The allocated bit set.
   */
  public static DirectBitSet allocate(long bits) {
    if (!(bits > 0 & (bits & (bits - 1)) == 0))
      throw new IllegalArgumentException("size must be a power of 2");
    return new DirectBitSet(new NativeBytes(allocator.allocate(bits / 64)));
  }

  private final NativeBytes bytes;
  private final long length;
  private long size;

  private DirectBitSet(NativeBytes bytes) {
    this(bytes, 0);
  }

  private DirectBitSet(NativeBytes bytes, long size) {
    this.bytes = bytes;
    this.length = bytes.size() * 64;
    this.size = size;
  }

  /**
   * Returns the offset for the given index.
   */
  private int offset(long index) {
    return (int) index >>> 6;
  }

  /**
   * Sets the bit at the given index.
   *
   * @param index The index of the bit to set.
   * @return Indicates if the bit was changed.
   */
  public boolean set(long index) {
    if (!get(index)) {
      int offset = offset(index);
      bytes.writeLong(offset, bytes.readLong(offset) | (1L << index));
      size++;
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
    return (bytes.readLong(offset(index)) & (1L << index)) != 0;
  }

  /**
   * Returns the total number of bits in the bit set.
   *
   * @return The total number of bits in the bit set.
   */
  public long length() {
    return length;
  }

  /**
   * Returns the number of bits set in the bit set.
   *
   * @return The number of bits set.
   */
  public long size() {
    return size;
  }

  /**
   * Copies the bit set to a new memory address.
   *
   * @return The copied bit set.
   */
  public DirectBitSet copy() {
    return new DirectBitSet(bytes.copy(), size);
  }

  @Override
  public void close() throws IOException {
    bytes.memory().free();
  }

}
