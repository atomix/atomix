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
package net.kuujo.copycat.io.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.BitArray;
import net.kuujo.copycat.io.util.Memory;

import java.io.IOException;

/**
 * Segment offset index.
 * <p>
 * The offset index handles indexing of entries in a given {@link Segment}. Given the offset and position of an entry
 * in a segment, the index will write the position to an underlying {@link net.kuujo.copycat.io.Buffer}. With this information, the index provides
 * useful metadata about the log such as the number of physical entries in the log and the first and last offsets.
 * <p>
 * Each entry in the index is stored in 8 bytes, a 1 byte status flag, a 24-bit unsigned offset, and a 32-bit unsigned
 * position. This places a limitation on the maximum indexed offset at {@code 2^31 - 1} and maximum indexed position at
 * {@code 2^32 - 1}.
 * <p>
 * When the index is first created, the {@link net.kuujo.copycat.io.Buffer} provided to the constructor will be scanned for existing entries.
 * <p>
 * The index assumes that entries will always be indexed in increasing order. However, this index also allows arbitrary
 * entries to be missing from the log due to log compaction. Because of the potential for missing entries, binary search
 * is used to locate positions rather than absolute positions. For efficiency, a {@link net.kuujo.copycat.io.MappedBuffer}
 * can be used to improve the performance of the binary search algorithm for persistent indexes.
 * <p>
 * In order to prevent searching the index for missing entries, all offsets are added to a memory efficient {@link net.kuujo.copycat.io.util.BitArray}
 * as they're written to the index. The bit array is sized according to the underlying index buffer. Prior to searching
 * for an offset in the index, the {@link net.kuujo.copycat.io.util.BitArray} is checked for existence of the offset in the index. Only if the offset
 * exists in the index is a binary search required.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class SearchableOffsetIndex implements OffsetIndex {

  /**
   * Returns the size of the index for the given number of entries.
   */
  public static long size(int maxEntries) {
    return (long) maxEntries * 8 + 16;
  }

  private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;
  private static final int HEADER_SIZE = 8;
  private static final int ENTRY_SIZE = 8;
  private static final int OFFSET_SIZE = 4;

  private static final long START = -1;
  private static final int END = -1;

  private final Buffer buffer;
  private final BitArray bits;
  private int size;
  private int firstOffset = -1;
  private int lastOffset = -1;
  private int currentOffset = -1;
  private long currentPosition = -1;
  private int currentLength = -1;

  public SearchableOffsetIndex(Buffer buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
    long bits = Memory.Util.toPow2(buffer.capacity() / ENTRY_SIZE);
    this.bits = BitArray.allocate(bits);
    init();
  }

  /**
   * Initializes the internal bit set.
   */
  private void init() {
    if (buffer.position(0).readLong() != START) {
      buffer.position(0).writeLong(START);
      return;
    }

    buffer.mark();

    int offset = buffer.readInt();
    while (offset != END) {
      if (firstOffset == -1) {
        firstOffset = offset;
      }

      lastOffset = offset;
      bits.set(offset % bits.length());
      size++;

      buffer.skip(4).mark();
      offset = buffer.readInt();
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

    if (position > MAX_POSITION) {
      throw new IllegalArgumentException("position cannot be greater than " + MAX_POSITION);
    }

    // If the length is zero, that indicates that this is a skipped entry. We don't index skipped entries at all.
    if (length == 0) {
      return;
    }

    buffer.writeInt(offset)
      .writeUnsignedInt(position)
      .mark()
      .writeInt(END)
      .writeInt(length)
      .reset();

    bits.set(offset % bits.length());

    if (firstOffset == -1) {
      firstOffset = offset;
    }

    size++;
    lastOffset = offset;

    if (currentOffset == offset) {
      currentPosition = currentLength = currentOffset = -1;
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(int offset) {
    return position(offset) != -1;
  }

  @Override
  public long position(int offset) {
    if (currentOffset == offset) {
      return currentPosition;
    }

    if (!bits.get(offset % bits.length())) {
      return -1;
    }

    int index = search(offset);

    currentOffset = offset;
    if (index == -1) {
      currentPosition = currentLength = -1;
    } else {
      currentPosition = buffer.readUnsignedInt(index + OFFSET_SIZE);

      int next = buffer.readInt(index + ENTRY_SIZE);
      if (next == END) {
        currentLength = buffer.readInt(index + ENTRY_SIZE + OFFSET_SIZE);
      } else {
        currentLength = (int) (buffer.readUnsignedInt(index + ENTRY_SIZE + OFFSET_SIZE) - currentPosition);
      }
    }
    return currentPosition;
  }

  @Override
  public int length(int offset) {
    if (currentOffset == offset)
      return currentLength;
    return position(offset) != -1 ? currentLength : -1;
  }

  /**
   * Performs a binary search to find the given offset in the buffer.
   */
  private int search(int offset) {
    if (size == 0) {
      return -1;
    }

    int lo = 0;
    int hi = size - 1;

    while (lo < hi) {
      int mid = lo + (hi - lo) / 2;
      int i = buffer.readInt(mid * ENTRY_SIZE + HEADER_SIZE);
      if (i == offset) {
        return mid * ENTRY_SIZE + HEADER_SIZE;
      } else if (lo == mid) {
        if (buffer.readInt(hi * ENTRY_SIZE + HEADER_SIZE) == offset) {
          return hi * ENTRY_SIZE + HEADER_SIZE;
        }
        return -1;
      } else if (i < offset) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }

    if (buffer.readInt(hi * ENTRY_SIZE + HEADER_SIZE) == offset) {
      return hi * ENTRY_SIZE + HEADER_SIZE;
    }
    return -1;
  }

  @Override
  public void truncate(int offset) {
    if (offset == lastOffset)
      return;

    int index = search(offset + 1);

    if (index == -1)
      throw new IllegalStateException("unknown offset: " + offset);

    int lastOffset = lastOffset();
    for (int i = lastOffset; i > offset; i--) {
      if (position(i) != -1) {
        size--;
      }
    }

    long previousPosition = buffer.readUnsignedInt(index - OFFSET_SIZE);
    long indexPosition = buffer.readUnsignedInt(index + OFFSET_SIZE);

    buffer.position(index)
      .zero(index)
      .mark()
      .writeByte(END)
      .writeInt((int) (indexPosition - previousPosition))
      .reset();
    this.lastOffset = offset;

    currentPosition = currentOffset = currentLength = -1;
  }

  @Override
  public void flush() {
    buffer.flush();
  }

  @Override
  public void close() {
    try {
      buffer.close();
      bits.close();
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

}
