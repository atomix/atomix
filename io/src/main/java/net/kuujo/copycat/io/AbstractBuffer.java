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

import net.kuujo.copycat.io.util.Memory;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractBuffer implements Buffer {
  static final long DEFAULT_INITIAL_CAPACITY = 4096;

  protected final Bytes bytes;
  private long offset;
  private long initialCapacity;
  private long capacity;
  private long maxCapacity;
  private long position;
  private long limit = -1;
  private long mark = -1;
  private final AtomicInteger references = new AtomicInteger();
  private final ReferenceManager<Buffer> referenceManager;
  private final BufferPool bufferPool = new BufferPool(this::createView);
  private final BufferReaderPool readerPool = new BufferReaderPool(this);
  private final BufferWriterPool writerPool = new BufferWriterPool(this);
  private final BufferPool readOnlyPool = new BufferPool(referenceManager -> new ReadOnlyBuffer(this, referenceManager));
  private SwappedBuffer swap;

  protected AbstractBuffer(Bytes bytes, ReferenceManager<Buffer> referenceManager) {
    this(bytes, 0, 0, 0, referenceManager);
  }

  protected AbstractBuffer(Bytes bytes, long offset, long initialCapacity, long maxCapacity, ReferenceManager<Buffer> referenceManager) {
    if (bytes == null)
      throw new NullPointerException("bytes cannot be null");
    if (offset < 0)
      throw new IndexOutOfBoundsException("offset out of bounds of the underlying byte array");
    this.bytes = bytes;
    this.offset = offset;
    this.capacity = 0;
    this.initialCapacity = initialCapacity;
    this.maxCapacity = maxCapacity;
    capacity(initialCapacity);
    this.referenceManager = referenceManager;
  }

  /**
   * Resets the buffer's internal offset and capacity.
   */
  protected AbstractBuffer view(long offset, long capacity, long maxCapacity) {
    this.offset = offset;
    this.capacity = 0;
    this.initialCapacity = capacity;
    this.maxCapacity = maxCapacity;
    capacity(initialCapacity);
    references.set(1);
    rewind();
    return this;
  }

  /**
   * Internal factory method for creating a view of this buffer.
   */
  protected abstract Buffer createView(ReferenceManager<Buffer> referenceManager);

  @Override
  public Buffer acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0) {
      if (referenceManager != null)
        referenceManager.release(this);
      else
        bytes.close();
    }
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  public Bytes bytes() {
    return bytes;
  }

  @Override
  public ByteOrder order() {
    return bytes.order();
  }

  @Override
  public Buffer order(ByteOrder order) {
    if (order == null)
      throw new NullPointerException("order cannot be null");
    if (order == order())
      return this;
    if (swap != null)
      return swap;
    swap = new SwappedBuffer(this, offset, capacity, maxCapacity, referenceManager);
    return swap;
  }

  @Override
  public boolean isDirect() {
    return bytes.isDirect();
  }

  @Override
  public boolean isFile() {
    return bytes.isFile();
  }

  @Override
  public Buffer asReadOnlyBuffer() {
    return ((ReadOnlyBuffer) readOnlyPool.acquire())
      .view(offset, capacity, maxCapacity)
      .position(position)
      .limit(limit);
  }

  @Override
  public BufferReader reader() {
    return reader(position, limit != -1 ? limit - position : Math.max(remaining(), Math.min(initialCapacity, maxCapacity - position)));
  }

  @Override
  public BufferReader reader(long offset) {
    return reader(offset, limit != -1 ? limit - offset : maxCapacity - offset);
  }

  @Override
  public BufferReader reader(long offset, long length) {
    checkRead(offset, length);
    return readerPool.acquire().reset(offset(offset), length);
  }

  @Override
  public BufferWriter writer() {
    return writer(position, limit != -1 ? limit - position : Math.max(remaining(), Math.min(initialCapacity, maxCapacity - position)));
  }

  @Override
  public BufferWriter writer(long offset) {
    return writer(offset, limit != -1 ? limit - offset : maxCapacity - offset);
  }

  @Override
  public BufferWriter writer(long offset, long length) {
    checkWrite(offset, length);
    return writerPool.acquire().reset(offset(offset), length);
  }

  @Override
  public Buffer slice() {
    long maxCapacity = this.maxCapacity - position;
    long capacity = Math.min(initialCapacity, bytes.size() - offset(position));
    if (limit != -1)
      capacity = maxCapacity = limit - position;
    return ((AbstractBuffer) bufferPool.acquire()).view(offset(position), capacity, maxCapacity);
  }

  @Override
  public Buffer slice(long length) {
    checkSlice(position, length);
    return ((AbstractBuffer) bufferPool.acquire()).view(offset(position), length, length);
  }

  @Override
  public Buffer slice(long offset, long length) {
    checkSlice(offset, length);
    return ((AbstractBuffer) bufferPool.acquire()).view(offset(offset), length, length);
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public long capacity() {
    return capacity;
  }

  /**
   * Updates the buffer capacity.
   */
  public Buffer capacity(long capacity) {
    if (capacity > maxCapacity) {
      throw new IllegalArgumentException("capacity cannot be greater than maximum capacity");
    } else if (capacity < this.capacity) {
      throw new IllegalArgumentException("capacity cannot be decreased");
    } else if (capacity != this.capacity) {
      // It's possible that the bytes could already meet the requirements of the capacity.
      if (offset(capacity) > bytes.size()) {
        bytes.resize(Memory.toPow2(offset(capacity)));
      }
      this.capacity = capacity;
    }
    return this;
  }

  @Override
  public long maxCapacity() {
    return maxCapacity;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public Buffer position(long position) {
    if (limit != -1 && position > limit) {
      throw new IllegalArgumentException("position cannot be greater than limit");
    } else if (limit == -1 && position > maxCapacity) {
      throw new IllegalArgumentException("position cannot be greater than capacity");
    }
    if (position > capacity)
      capacity(Math.min(maxCapacity, Memory.toPow2(position)));
    this.position = position;
    return this;
  }

  /**
   * Returns the real offset of the given relative offset.
   */
  private long offset(long offset) {
    return this.offset + offset;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  public Buffer limit(long limit) {
    if (limit > maxCapacity)
      throw new IllegalArgumentException("limit cannot be greater than buffer capacity");
    if (limit < -1)
      throw new IllegalArgumentException("limit cannot be negative");
    if (limit != -1 && offset(limit) > bytes.size())
      bytes.resize(offset(limit));
    this.limit = limit;
    return this;
  }

  @Override
  public long remaining() {
    return (limit == -1 ? maxCapacity : limit) - position;
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public Buffer flip() {
    limit = position;
    position = 0;
    mark = -1;
    return this;
  }

  @Override
  public Buffer mark() {
    this.mark = position;
    return this;
  }

  @Override
  public Buffer rewind() {
    position = 0;
    mark = -1;
    return this;
  }

  @Override
  public Buffer reset() {
    if (mark == -1)
      throw new InvalidMarkException();
    position = mark;
    return this;
  }

  @Override
  public Buffer skip(long length) {
    if (length > remaining())
      throw new IndexOutOfBoundsException("length cannot be greater than remaining bytes in the buffer");
    position += length;
    return this;
  }

  @Override
  public Buffer clear() {
    position = 0;
    limit = -1;
    mark = -1;
    return this;
  }

  /**
   * Checks that the offset is within the bounds of the buffer.
   */
  protected void checkOffset(long offset) {
    if (offset(offset) < this.offset) {
      throw new IndexOutOfBoundsException();
    } else if (limit == -1) {
      if (offset > maxCapacity)
        throw new IndexOutOfBoundsException();
    } else {
      if (offset > limit)
        throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Checks bounds for a slice.
   */
  protected long checkSlice(long offset, long length) {
    checkOffset(offset);
    if (limit == -1) {
      if (offset + length > capacity) {
        if (capacity < maxCapacity) {
          capacity(Math.min(maxCapacity, capacity << 1));
        } else {
          throw new BufferUnderflowException();
        }
      }
    } else {
      if (offset + length > limit)
        throw new BufferUnderflowException();
    }
    return offset(offset);
  }

  /**
   * Checks bounds for a read for the given length.
   */
  protected long checkRead(long length) {
    checkRead(position, length);
    long previousPosition = this.position;
    this.position = previousPosition + length;
    return offset(previousPosition);
  }

  /**
   * Checks bounds for a read.
   */
  protected long checkRead(long offset, long length) {
    checkOffset(offset);
    if (limit == -1) {
      if (offset + length > capacity) {
        if (capacity < maxCapacity) {
          if (this.offset + offset + length <= bytes.size()) {
            capacity = bytes.size();
          } else {
            capacity(Math.min(maxCapacity, Math.max(capacity << 1, offset + length)));
          }
        } else {
          throw new BufferUnderflowException();
        }
      }
    } else {
      if (offset + length > limit)
        throw new BufferUnderflowException();
    }
    return offset(offset);
  }

  /**
   * Checks bounds for a write of the given length.
   */
  protected long checkWrite(long length) {
    checkWrite(position, length);
    long previousPosition = this.position;
    this.position = previousPosition + length;
    return offset(previousPosition);
  }

  /**
   * Checks bounds for a write.
   */
  protected long checkWrite(long offset, long length) {
    checkOffset(offset);
    if (limit == -1) {
      if (offset + length > capacity) {
        if (capacity < maxCapacity) {
          capacity(Math.min(maxCapacity, capacity << 1));
        } else {
          throw new BufferOverflowException();
        }
      }
    } else {
      if (offset + length > limit)
        throw new BufferOverflowException();
    }
    return offset(offset);
  }

  @Override
  public Buffer zero() {
    bytes.zero();
    return this;
  }

  @Override
  public Buffer zero(long offset) {
    bytes.zero(offset);
    return this;
  }

  @Override
  public Buffer zero(long offset, long length) {
    bytes.zero(offset, length);
    return this;
  }

  @Override
  public Buffer read(Buffer buffer) {
    long length = Math.min(buffer.remaining(), remaining());
    read(buffer.bytes(), buffer.offset() + buffer.position(), length);
    buffer.position(buffer.position() + length);
    return this;
  }

  @Override
  public Buffer read(BufferWriter writer) {
    long length = Math.min(writer.remaining(), remaining());
    read(writer.bytes(), writer.offset() + writer.position(), length);
    writer.position(writer.position() + length);
    return this;
  }

  @Override
  public Buffer read(Bytes bytes) {
    this.bytes.read(checkRead(bytes.size()), bytes, 0, bytes.size());
    return this;
  }

  @Override
  public Buffer read(Bytes bytes, long offset, long length) {
    this.bytes.read(checkRead(length), bytes, offset, length);
    return this;
  }

  @Override
  public Buffer read(long srcOffset, Bytes bytes, long dstOffset, long length) {
    this.bytes.read(checkRead(srcOffset, length), bytes, dstOffset, length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes) {
    this.bytes.read(checkRead(bytes.length), bytes, 0, bytes.length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes, long offset, long length) {
    this.bytes.read(checkRead(length), bytes, offset, length);
    return this;
  }

  @Override
  public Buffer read(long srcOffset, byte[] bytes, long dstOffset, long length) {
    this.bytes.read(checkRead(srcOffset, length), bytes, dstOffset, length);
    return this;
  }

  @Override
  public int readByte() {
    return bytes.readByte(checkRead(Byte.BYTES));
  }

  @Override
  public int readByte(long offset) {
    return bytes.readByte(checkRead(offset, Byte.BYTES));
  }

  @Override
  public int readUnsignedByte() {
    return bytes.readUnsignedByte(checkRead(Byte.BYTES));
  }

  @Override
  public int readUnsignedByte(long offset) {
    return bytes.readUnsignedByte(checkRead(offset, Byte.BYTES));
  }

  @Override
  public char readChar() {
    return bytes.readChar(checkRead(Character.BYTES));
  }

  @Override
  public char readChar(long offset) {
    return bytes.readChar(checkRead(offset, Character.BYTES));
  }

  @Override
  public short readShort() {
    return bytes.readShort(checkRead(Short.BYTES));
  }

  @Override
  public short readShort(long offset) {
    return bytes.readShort(checkRead(offset, Short.BYTES));
  }

  @Override
  public int readUnsignedShort() {
    return bytes.readUnsignedShort(checkRead(Short.BYTES));
  }

  @Override
  public int readUnsignedShort(long offset) {
    return bytes.readUnsignedShort(checkRead(offset, Short.BYTES));
  }

  @Override
  public int readMedium() {
    return bytes.readMedium(checkRead(3));
  }

  @Override
  public int readMedium(long offset) {
    return bytes.readMedium(checkRead(offset, 3));
  }

  @Override
  public int readUnsignedMedium() {
    return bytes.readUnsignedMedium(checkRead(3));
  }

  @Override
  public int readUnsignedMedium(long offset) {
    return bytes.readUnsignedMedium(checkRead(offset, 3));
  }

  @Override
  public int readInt() {
    return bytes.readInt(checkRead(Integer.BYTES));
  }

  @Override
  public int readInt(long offset) {
    return bytes.readInt(checkRead(offset, Integer.BYTES));
  }

  @Override
  public long readUnsignedInt() {
    return bytes.readUnsignedInt(checkRead(Integer.BYTES));
  }

  @Override
  public long readUnsignedInt(long offset) {
    return bytes.readUnsignedInt(checkRead(offset, Integer.BYTES));
  }

  @Override
  public long readLong() {
    return bytes.readLong(checkRead(Long.BYTES));
  }

  @Override
  public long readLong(long offset) {
    return bytes.readLong(checkRead(offset, Long.BYTES));
  }

  @Override
  public float readFloat() {
    return bytes.readFloat(checkRead(Float.BYTES));
  }

  @Override
  public float readFloat(long offset) {
    return bytes.readFloat(checkRead(offset, Float.BYTES));
  }

  @Override
  public double readDouble() {
    return bytes.readDouble(checkRead(Double.BYTES));
  }

  @Override
  public double readDouble(long offset) {
    return bytes.readDouble(checkRead(offset, Double.BYTES));
  }

  @Override
  public boolean readBoolean() {
    return bytes.readBoolean(checkRead(Byte.BYTES));
  }

  @Override
  public boolean readBoolean(long offset) {
    return bytes.readBoolean(checkRead(offset, Byte.BYTES));
  }

  @Override
  public Buffer write(Buffer buffer) {
    long length = Math.min(buffer.remaining(), remaining());
    write(buffer.bytes(), buffer.offset() + buffer.position(), length);
    buffer.position(buffer.position() + length);
    return this;
  }

  @Override
  public Buffer write(BufferReader reader) {
    long length = Math.min(reader.remaining(), remaining());
    write(reader.bytes(), reader.offset() + reader.position(), length);
    reader.position(reader.position() + length);
    return this;
  }

  @Override
  public Buffer write(Bytes bytes) {
    this.bytes.write(checkWrite(bytes.size()), bytes, 0, bytes.size());
    return this;
  }

  @Override
  public Buffer write(Bytes bytes, long offset, long length) {
    this.bytes.write(checkWrite(length), bytes, offset, length);
    return this;
  }

  @Override
  public Buffer write(long offset, Bytes bytes, long srcOffset, long length) {
    this.bytes.write(checkWrite(offset, length), bytes, srcOffset, length);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes) {
    this.bytes.write(checkWrite(bytes.length), bytes, 0, bytes.length);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes, long offset, long length) {
    this.bytes.write(checkWrite(length), bytes, offset, length);
    return this;
  }

  @Override
  public Buffer write(long offset, byte[] bytes, long srcOffset, long length) {
    this.bytes.write(checkWrite(offset, length), bytes, srcOffset, length);
    return this;
  }

  @Override
  public Buffer writeByte(int b) {
    bytes.writeByte(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer writeByte(long offset, int b) {
    bytes.writeByte(checkWrite(offset, Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(int b) {
    bytes.writeUnsignedByte(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(long offset, int b) {
    bytes.writeUnsignedByte(checkWrite(offset, Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer writeChar(char c) {
    bytes.writeChar(checkWrite(Character.BYTES), c);
    return this;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    bytes.writeChar(checkWrite(offset, Character.BYTES), c);
    return this;
  }

  @Override
  public Buffer writeShort(short s) {
    bytes.writeShort(checkWrite(Short.BYTES), s);
    return this;
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    bytes.writeShort(checkWrite(offset, Short.BYTES), s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(int s) {
    bytes.writeUnsignedShort(checkWrite(Short.BYTES), s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(long offset, int s) {
    bytes.writeUnsignedShort(checkWrite(offset, Short.BYTES), s);
    return this;
  }

  @Override
  public Buffer writeMedium(int m) {
    bytes.writeMedium(checkWrite(3), m);
    return this;
  }

  @Override
  public Buffer writeMedium(long offset, int m) {
    bytes.writeMedium(checkWrite(offset, 3), m);
    return this;
  }

  @Override
  public Buffer writeUnsignedMedium(int m) {
    bytes.writeUnsignedMedium(checkWrite(3), m);
    return this;
  }

  @Override
  public Buffer writeUnsignedMedium(long offset, int m) {
    bytes.writeUnsignedMedium(checkWrite(offset, 3), m);
    return this;
  }

  @Override
  public Buffer writeInt(int i) {
    bytes.writeInt(checkWrite(Integer.BYTES), i);
    return this;
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    bytes.writeInt(checkWrite(offset, Integer.BYTES), i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long i) {
    bytes.writeUnsignedInt(checkWrite(Integer.BYTES), i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long offset, long i) {
    bytes.writeUnsignedInt(checkWrite(offset, Integer.BYTES), i);
    return this;
  }

  @Override
  public Buffer writeLong(long l) {
    bytes.writeLong(checkWrite(Long.BYTES), l);
    return this;
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    bytes.writeLong(checkWrite(offset, Long.BYTES), l);
    return this;
  }

  @Override
  public Buffer writeFloat(float f) {
    bytes.writeFloat(checkWrite(Float.BYTES), f);
    return this;
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    bytes.writeFloat(checkWrite(offset, Float.BYTES), f);
    return this;
  }

  @Override
  public Buffer writeDouble(double d) {
    bytes.writeDouble(checkWrite(Double.BYTES), d);
    return this;
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    bytes.writeDouble(checkWrite(offset, Double.BYTES), d);
    return this;
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    bytes.writeBoolean(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    bytes.writeBoolean(checkWrite(offset, Byte.BYTES), b);
    return this;
  }

  @Override
  public Buffer flush() {
    bytes.flush();
    return this;
  }

  @Override
  public void close() {
    release();
  }

}
