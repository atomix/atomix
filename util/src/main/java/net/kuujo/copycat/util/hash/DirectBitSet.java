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
package net.kuujo.copycat.util.hash;

import net.kuujo.copycat.util.internal.Assert;

import java.io.Closeable;
import java.io.IOException;

import static net.kuujo.copycat.util.NativeMemory.UNSAFE;

/**
 * Direct memory bit set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBitSet implements Closeable {
  private final long size;
  private long address;
  private long length;

  public DirectBitSet(long size) {
    this.size = Assert.arg(size, size > 0 & (size & (size - 1)) == 0, "size must be a power of 2");
    this.address = UNSAFE.allocateMemory(size);
  }

  private DirectBitSet(long size, long length, long address) {
    this.size = size;
    this.length = length;
    this.address = address;
  }

  /**
   * Sets the bit at the given index.
   *
   * @param index The index of the bit to set.
   * @return Indicates if the bit was changed.
   */
  public boolean set(long index) {
    if (!get(index)) {
      UNSAFE.putLong(address + ((int) index >>> 6), UNSAFE.getLong(address + ((int) index >>> 6)) | (1L << index));
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
    return (UNSAFE.getLong(address + ((int) index >>> 6)) & (1L << index)) != 0;
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
    long address = UNSAFE.allocateMemory(size);
    UNSAFE.copyMemory(this.address, address, size);
    return new DirectBitSet(size, length, address);
  }

  @Override
  public void close() throws IOException {
    if (address > 0) {
      UNSAFE.freeMemory(address);
      address = 0;
    }
  }

}
