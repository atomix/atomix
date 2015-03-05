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

import java.io.Closeable;
import java.io.IOException;

/**
 * Direct memory bit set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBitSet implements Closeable {
  private static final NativeAllocator allocator = new NativeAllocator();
  private final NativeBytes bytes;
  private long length;

  public DirectBitSet(long size) {
    if (!(size > 0 & (size & (size - 1)) == 0))
      throw new IllegalArgumentException("size must be a power of 2");
    this.bytes = new NativeBytes(allocator.allocate(size));
  }

  private DirectBitSet(NativeBytes bytes, long length) {
    this.bytes = bytes;
    this.length = length;
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
      length++;
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
   * Returns the number of bits set in the bit set.
   *
   * @return The number of bits set.
   */
  public long size() {
    return length;
  }

  /**
   * Copies the bit set to a new memory address.
   *
   * @return The copied bit set.
   */
  public DirectBitSet copy() {
    return new DirectBitSet(bytes.copy(), length);
  }

  @Override
  public void close() throws IOException {
    bytes.memory().free();
  }

}
