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
package net.kuujo.copycat.io;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

/**
 * Buffer navigator helper class.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class BufferNavigator {
  private final long offset;
  private final long capacity;
  private long position;
  private long limit;
  private long mark;

  BufferNavigator(long offset, long capacity) {
    this.offset = offset;
    this.capacity = capacity;
    this.limit = capacity;
    clear();
  }

  /**
   * Returns the buffer capacity.
   */
  long capacity() {
    return capacity;
  }

  /**
   * Returns the current position.
   */
  long position() {
    return position;
  }

  /**
   * Sets the current position.
   */
  BufferNavigator position(long position) {
    if (position > limit)
      throw new IllegalArgumentException("position cannot be greater than limit");
    this.position = position;
    return this;
  }

  /**
   * Returns the current limit.
   */
  long limit() {
    return limit;
  }

  /**
   * Sets the current limit.
   */
  BufferNavigator limit(long limit) {
    if (limit > capacity)
      throw new IllegalArgumentException("limit cannot be greater than buffer capacity");
    this.limit = limit;
    return this;
  }

  /**
   * Returns the number of bytes remaining.
   */
  long remaining() {
    return limit - position;
  }

  /**
   * Indicates whether there are bytes remaining.
   */
  boolean hasRemaining() {
    return limit - position > 0;
  }

  /**
   * Flips the navigator.
   */
  BufferNavigator flip() {
    limit = position;
    position = offset;
    mark = position;
    return this;
  }

  /**
   * Sets a mark.
   */
  BufferNavigator mark() {
    this.mark = position;
    return this;
  }

  /**
   * Rewinds the navigator.
   */
  BufferNavigator rewind() {
    position = offset;
    mark = position;
    return this;
  }

  /**
   * Resets the navigator.
   */
  BufferNavigator reset() {
    position = mark;
    return this;
  }

  /**
   * Clears the navigator.
   */
  BufferNavigator clear() {
    position = offset;
    limit = capacity;
    mark = position;
    return this;
  }

  /**
   * Checks that the offset is within the bounds of the buffer.
   */
  void checkOffset(long offset) {
    if (offset < this.offset || offset > limit)
      throw new IndexOutOfBoundsException();
  }

  /**
   * Checks bounds for a read for the given length.
   */
  long checkRead(long length) {
    return checkRead(position, length);
  }

  /**
   * Checks bounds for a read.
   */
  long checkRead(long offset, long length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > limit)
      throw new BufferUnderflowException();
    return position;
  }

  /**
   * Checks bounds for a write of the given length.
   */
  long checkWrite(long length) {
    return checkWrite(position, length);
  }

  /**
   * Checks bounds for a write.
   */
  long checkWrite(long offset, long length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > limit)
      throw new BufferOverflowException();
    return position;
  }

  /**
   * Sets the position.
   */
  long getAndSetPosition(long position) {
    long previousPosition = this.position;
    this.position = position;
    return previousPosition;
  }

}
