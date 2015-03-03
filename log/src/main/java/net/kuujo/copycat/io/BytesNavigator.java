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
 * Base buffer navigator implementation.
 * <p>
 * This is a simple {@link BytesNavigator} implementation that provides the common behaviors
 * required of navigating buffers. Additionally, because the state within the {@code BufferNavigator} is unique and
 * external to the underlying buffer, users can use the {@code BufferNavigator} to wrap an existing buffer and maintain
 * multiple positions on top of that buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BytesNavigator<T extends NavigableBytes<?>> implements NavigableBytes<T> {
  private long capacity;
  private long position;
  private long limit;
  private long mark;

  public BytesNavigator(long capacity) {
    if (capacity < 0)
      throw new IllegalArgumentException("capacity cannot be negative");
    this.capacity = capacity;
    this.limit = capacity;
  }

  /**
   * Checks that the offset is within the bounds of the buffer.
   */
  private void checkOffset(long offset) {
    if (offset < 0 || offset > limit)
      throw new IndexOutOfBoundsException();
  }

  /**
   * Checks bounds for a read.
   */
  protected long checkRead(long offset, int length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > limit)
      throw new BufferUnderflowException();
    return position;
  }

  /**
   * Checks bounds for a write.
   */
  protected long checkWrite(long offset, int length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > limit)
      throw new BufferOverflowException();
    return position;
  }

  /**
   * Sets the position.
   */
  protected long getAndSetPosition(long position) {
    long previousPosition = this.position;
    this.position = position;
    return previousPosition;
  }

  @Override
  public long capacity() {
    return capacity;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T position(long position) {
    if (position > limit)
      throw new IllegalArgumentException("position cannot be greater than limit");
    this.position = position;
    return (T) this;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T limit(long limit) {
    if (limit > capacity)
      throw new IllegalArgumentException("limit cannot be greater than buffer capacity");
    this.limit = limit;
    return (T) this;
  }

  @Override
  public long remaining() {
    return limit - position;
  }

  @Override
  public boolean hasRemaining() {
    return limit - position > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T flip() {
    limit = position;
    position = 0;
    mark = position;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T mark() {
    this.mark = position;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T rewind() {
    position = 0;
    mark = position;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T reset() {
    position = mark;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T clear() {
    position = 0;
    limit = capacity;
    mark = position;
    return (T) this;
  }

}
