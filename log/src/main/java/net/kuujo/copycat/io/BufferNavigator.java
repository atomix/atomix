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

/**
 * Buffer navigator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferNavigator<T extends NavigableBuffer<T>> implements NavigableBuffer<T> {
  private final Buffer buffer;
  private long position;
  private long limit;
  private long mark;

  public BufferNavigator(Buffer buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
    this.limit = buffer.capacity();
  }

  /**
   * Checks the bounds of the given offset.
   */
  protected long checkBounds(long offset) {
    if (offset > limit)
      throw new IllegalArgumentException("offset is greater than the block limit");
    return offset;
  }

  /**
   * Checks the bounds of the given offset given the number of bytes.
   */
  protected long checkBounds(long offset, int bytes) {
    return checkBounds(offset + bytes);
  }

  /**
   * Returns the current position and increments the position by the given number of bytes.
   */
  protected long incrementPosition(int bytes) {
    long position = this.position;
    this.position += bytes;
    return position;
  }

  @Override
  public long capacity() {
    return buffer.capacity();
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
    if (limit > buffer.capacity())
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
  public T mark(long mark) {
    checkBounds(mark);
    this.mark = mark;
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
    limit = buffer.capacity();
    mark = position;
    return (T) this;
  }

}
