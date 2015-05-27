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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.io.Buffer;

/**
 * Offset index that expects all entries in the log to be indexed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class OrderedOffsetIndex implements OffsetIndex {
  static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;

  private static final long END = -1;

  private final Buffer buffer;
  private int firstOffset;
  private int lastOffset;
  private int size;

  public OrderedOffsetIndex(Buffer buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
    init();
  }

  /**
   * Initializes the index.
   */
  private void init() {
    buffer.mark();
    long position = buffer.readUnsignedInt();

    while (position != END) {
      if (firstOffset == -1) {
        firstOffset = 0;
      }
      lastOffset++;
      position = buffer.mark().readUnsignedInt();
    }

    buffer.reset();

    if (size == 0) {
      buffer.position(0).writeUnsignedInt(END);
    }
  }

  @Override
  public int firstOffset() {
    return firstOffset;
  }

  @Override
  public int lastOffset() {
    return lastOffset;
  }

  @Override
  public void index(int offset, long position, int length) {
    if (lastOffset > -1 && offset <= lastOffset)
      throw new IllegalArgumentException("offset cannot be less than or equal to the last offset in the index");
    if (offset != lastOffset + 1)
      throw new IllegalArgumentException("offset must be monotonically increasing");
    if (position > MAX_POSITION)
      throw new IllegalArgumentException("position cannot be greater than " + MAX_POSITION);

    // Write a status byte, offset, and position, then write the end byte and length.
    buffer.writeUnsignedInt(position)
      .mark()
      .writeUnsignedInt(END)
      .writeInt(length)
      .reset();

    if (firstOffset == -1) {
      firstOffset = offset;
    }

    this.size++;
    this.lastOffset = offset;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(int offset) {
    return lastOffset <= offset;
  }

  @Override
  public long position(int offset) {
    if (offset < firstOffset || offset > lastOffset)
      throw new IllegalArgumentException("offset cannot be less that first offset or greater than last offset");
    return buffer.readUnsignedInt(offset * 4);
  }

  @Override
  public int length(int offset) {
    if (offset == lastOffset) {
      return buffer.readInt(offset * 4 + 4);
    } else {
      return (int) (buffer.readUnsignedInt(offset * 4 + 4) - buffer.readUnsignedInt(offset * 4));
    }
  }

  @Override
  public void truncate(int offset) {
    if (offset >= firstOffset && offset < lastOffset) {
      int length = length(offset);
      buffer.position(offset * 4 + 4)
        .zero(offset * 4 + 4)
        .mark()
        .writeUnsignedInt(END)
        .writeInt(length)
        .reset();
      size -= lastOffset - offset;
    }
  }

  @Override
  public void flush() {
    buffer.flush();
  }

  @Override
  public void close() {
    buffer.close();
  }

}
