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

  /**
   * Returns the size of the index for the given number of entries.
   */
  public static long size(int maxEntries) {
    return (long) maxEntries * 4l + 12l;
  }

  private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;
  private static final int HEADER_SIZE = 8;
  private static final long START = -1;
  private static final long END = 0;

  private final Buffer buffer;
  private int firstOffset = -1;
  private int lastOffset = -1;
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
    // If no entries have been written to the index then the START flag will not exist.
    if (buffer.position(0).readLong() != START) {
      return;
    }

    // We assume that the first entry in the index is always at position 0. If the START flag is written, then entry
    // 0 exists in the index.
    firstOffset = lastOffset = 0;
    size = 1;

    long position = buffer.readUnsignedInt();
    while (position != 0) {
      lastOffset++;
      size++;
      position = buffer.mark().readUnsignedInt();
    }

    buffer.reset();
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
    if (lastOffset > -1 && offset <= lastOffset) {
      throw new IllegalArgumentException("offset cannot be less than or equal to the last offset in the index");
    }
    if (offset != lastOffset + 1) {
      throw new IllegalArgumentException("offset must be monotonically increasing");
    }
    if (position > MAX_POSITION) {
      throw new IllegalArgumentException("position cannot be greater than " + MAX_POSITION);
    }

    if (offset == 0) {
      if (position != 0) {
        throw new IllegalArgumentException("offset 0 must be at position 0");
      }
      buffer.rewind().writeLong(START);
      firstOffset = 0;
    } else {
      buffer.writeUnsignedInt(position);
    }

    buffer.mark()
      .writeUnsignedInt(END)
      .writeInt(length)
      .reset();

    size++;
    lastOffset = offset;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(int offset) {
    return lastOffset >= offset;
  }

  @Override
  public long position(int offset) {
    if (offset < firstOffset || offset > lastOffset) {
      throw new IllegalArgumentException("offset cannot be less that first offset or greater than last offset");
    }

    if (offset == 0) {
      return size >= 0 ? 0 : -1;
    }
    return buffer.readUnsignedInt(HEADER_SIZE + offset * Integer.BYTES - Integer.BYTES);
  }

  @Override
  public int length(int offset) {
    if (offset == lastOffset) {
      return buffer.readInt(HEADER_SIZE + offset * Integer.BYTES + Integer.BYTES);
    } else if (offset == 0) {
      return buffer.readInt(HEADER_SIZE);
    } else {
      return (int) (position(offset + 1) - position(offset));
    }
  }

  @Override
  public void truncate(int offset) {
    if (offset == -1) {
      buffer.zero();
    } else if (offset >= firstOffset && offset < lastOffset) {
      int length = length(offset);
      if (offset == 0) {
        buffer.rewind()
          .writeLong(START)
          .mark()
          .writeUnsignedInt(END)
          .writeInt(length)
          .reset();
        size = 1;
        lastOffset = 0;
      } else {
        buffer.position(HEADER_SIZE + offset * Integer.BYTES)
          .zero(HEADER_SIZE + offset * Integer.BYTES + Integer.BYTES)
          .mark()
          .writeUnsignedInt(END)
          .writeInt(length)
          .reset();

        size -= lastOffset - offset;
        lastOffset = offset;
      }
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
