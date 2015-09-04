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
import net.kuujo.copycat.util.ReferenceManager;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractBuffer implements Buffer {
  static final long DEFAULT_INITIAL_CAPACITY = 4096;

  protected final Bytes bytes;
  private byte[] chars = new byte[0];
  private long offset;
  private long initialCapacity;
  private long capacity;
  private long maxCapacity;
  private long position;
  private long limit = -1;
  private long mark = -1;
  private final AtomicInteger references = new AtomicInteger();
  private final ReferenceManager<Buffer> referenceManager;
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
    references.set(1);
  }

  /**
   * Resets the buffer's internal offset and capacity.
   */
  protected AbstractBuffer reset(long offset, long capacity, long maxCapacity) {
    this.offset = offset;
    this.capacity = 0;
    this.initialCapacity = capacity;
    this.maxCapacity = maxCapacity;
    capacity(initialCapacity);
    references.set(0);
    rewind();
    return this;
  }

  @Override
  public Buffer acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public boolean release() {
    if (references.decrementAndGet() == 0) {
      if (referenceManager != null)
        referenceManager.release(this);
      else
        bytes.close();
      return true;
    }
    return false;
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
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public Buffer asReadOnlyBuffer() {
    return new ReadOnlyBuffer(this, referenceManager)
      .reset(offset, capacity, maxCapacity)
      .position(position)
      .limit(limit);
  }

  @Override
  public Buffer compact() {
    compact(offset(position), offset, (limit != -1 ? limit : capacity) - offset(position));
    return clear();
  }

  /**
   * Compacts the given bytes.
   */
  protected abstract void compact(long from, long to, long length);

  @Override
  public Buffer slice() {
    long maxCapacity = this.maxCapacity - position;
    long capacity = Math.min(Math.min(initialCapacity, maxCapacity), bytes.size() - offset(position));
    if (limit != -1)
      capacity = maxCapacity = limit - position;
    return new SlicedBuffer(this, bytes, offset(position), capacity, maxCapacity);
  }

  @Override
  public Buffer slice(long length) {
    checkSlice(position, length);
    return new SlicedBuffer(this, bytes, offset(position), length, length);
  }

  @Override
  public Buffer slice(long offset, long length) {
    checkSlice(offset, length);
    return new SlicedBuffer(this, bytes, offset(offset), length, length);
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
        bytes.resize(Memory.Util.toPow2(offset(capacity)));
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
      capacity(Math.min(maxCapacity, Memory.Util.toPow2(position)));
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
          capacity(calculateCapacity(offset + length));
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
            capacity(calculateCapacity(offset + length));
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
          capacity(calculateCapacity(offset + length));
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

  /**
   * Calculates the next capacity that meets the given minimum capacity.
   */
  private long calculateCapacity(long minimumCapacity) {
    long newCapacity = capacity;
    while (newCapacity < Math.min(minimumCapacity, maxCapacity)) {
      newCapacity <<= 1;
    }
    return Math.min(newCapacity, maxCapacity);
  }

  @Override
  public Buffer zero() {
    bytes.zero(offset);
    return this;
  }

  @Override
  public Buffer zero(long offset) {
    checkOffset(offset);
    bytes.zero(offset(offset));
    return this;
  }

  @Override
  public Buffer zero(long offset, long length) {
    checkOffset(offset);
    bytes.zero(offset(offset), length);
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
    return bytes.readByte(checkRead(Bytes.BYTE));
  }

  @Override
  public int readByte(long offset) {
    return bytes.readByte(checkRead(offset, Bytes.BYTE));
  }

  @Override
  public int readUnsignedByte() {
    return bytes.readUnsignedByte(checkRead(Bytes.BYTE));
  }

  @Override
  public int readUnsignedByte(long offset) {
    return bytes.readUnsignedByte(checkRead(offset, Bytes.BYTE));
  }

  @Override
  public char readChar() {
    return bytes.readChar(checkRead(Bytes.CHARACTER));
  }

  @Override
  public char readChar(long offset) {
    return bytes.readChar(checkRead(offset, Bytes.CHARACTER));
  }

  @Override
  public short readShort() {
    return bytes.readShort(checkRead(Bytes.SHORT));
  }

  @Override
  public short readShort(long offset) {
    return bytes.readShort(checkRead(offset, Bytes.SHORT));
  }

  @Override
  public int readUnsignedShort() {
    return bytes.readUnsignedShort(checkRead(Bytes.SHORT));
  }

  @Override
  public int readUnsignedShort(long offset) {
    return bytes.readUnsignedShort(checkRead(offset, Bytes.SHORT));
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
    return bytes.readInt(checkRead(Bytes.INTEGER));
  }

  @Override
  public int readInt(long offset) {
    return bytes.readInt(checkRead(offset, Bytes.INTEGER));
  }

  @Override
  public long readUnsignedInt() {
    return bytes.readUnsignedInt(checkRead(Bytes.INTEGER));
  }

  @Override
  public long readUnsignedInt(long offset) {
    return bytes.readUnsignedInt(checkRead(offset, Bytes.INTEGER));
  }

  @Override
  public long readLong() {
    return bytes.readLong(checkRead(Bytes.LONG));
  }

  @Override
  public long readLong(long offset) {
    return bytes.readLong(checkRead(offset, Bytes.LONG));
  }

  @Override
  public float readFloat() {
    return bytes.readFloat(checkRead(Bytes.FLOAT));
  }

  @Override
  public float readFloat(long offset) {
    return bytes.readFloat(checkRead(offset, Bytes.FLOAT));
  }

  @Override
  public double readDouble() {
    return bytes.readDouble(checkRead(Bytes.DOUBLE));
  }

  @Override
  public double readDouble(long offset) {
    return bytes.readDouble(checkRead(offset, Bytes.DOUBLE));
  }

  @Override
  public boolean readBoolean() {
    return bytes.readBoolean(checkRead(Bytes.BYTE));
  }

  @Override
  public boolean readBoolean(long offset) {
    return bytes.readBoolean(checkRead(offset, Bytes.BYTE));
  }

  @Override
  public String readString() {
    if (readByte() != 0) {
      int length = readUnsignedShort();
      if (length > chars.length) {
        chars = new byte[length];
      }
      read(chars, 0, length);
      return new String(chars, 0, length);
    }
    return null;
  }

  @Override
  public String readString(long offset) {
    if (readByte(offset) != 0) {
      int length = readUnsignedShort(offset + Bytes.BYTE);
      if (length > chars.length) {
        chars = new byte[length];
      }
      read(offset + Bytes.BYTE + Bytes.SHORT, chars, 0, length);
      return new String(chars, 0, length);
    }
    return null;
  }

  @Override
  public String readUTF8() {
    if (readByte() != 0) {
      int length = readUnsignedShort();
      if (length > chars.length) {
        chars = new byte[length];
      }
      read(chars, 0, length);
      return new String(chars, 0, length, StandardCharsets.UTF_8);
    }
    return null;
  }

  @Override
  public String readUTF8(long offset) {
    if (readByte(offset) != 0) {
      int length = readUnsignedShort(offset + Bytes.BYTE);
      if (length > chars.length) {
        chars = new byte[length];
      }
      read(offset + Bytes.BYTE + Bytes.SHORT, chars, 0, length);
      return new String(chars, 0, length, StandardCharsets.UTF_8);
    }
    return null;
  }

  @Override
  public Buffer write(Buffer buffer) {
    long length = Math.min(buffer.remaining(), remaining());
    write(buffer.bytes(), buffer.offset() + buffer.position(), length);
    buffer.position(buffer.position() + length);
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
    bytes.writeByte(checkWrite(Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeByte(long offset, int b) {
    bytes.writeByte(checkWrite(offset, Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(int b) {
    bytes.writeUnsignedByte(checkWrite(Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(long offset, int b) {
    bytes.writeUnsignedByte(checkWrite(offset, Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeChar(char c) {
    bytes.writeChar(checkWrite(Bytes.CHARACTER), c);
    return this;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    bytes.writeChar(checkWrite(offset, Bytes.CHARACTER), c);
    return this;
  }

  @Override
  public Buffer writeShort(short s) {
    bytes.writeShort(checkWrite(Bytes.SHORT), s);
    return this;
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    bytes.writeShort(checkWrite(offset, Bytes.SHORT), s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(int s) {
    bytes.writeUnsignedShort(checkWrite(Bytes.SHORT), s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(long offset, int s) {
    bytes.writeUnsignedShort(checkWrite(offset, Bytes.SHORT), s);
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
    bytes.writeInt(checkWrite(Bytes.INTEGER), i);
    return this;
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    bytes.writeInt(checkWrite(offset, Bytes.INTEGER), i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long i) {
    bytes.writeUnsignedInt(checkWrite(Bytes.INTEGER), i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long offset, long i) {
    bytes.writeUnsignedInt(checkWrite(offset, Bytes.INTEGER), i);
    return this;
  }

  @Override
  public Buffer writeLong(long l) {
    bytes.writeLong(checkWrite(Bytes.LONG), l);
    return this;
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    bytes.writeLong(checkWrite(offset, Bytes.LONG), l);
    return this;
  }

  @Override
  public Buffer writeFloat(float f) {
    bytes.writeFloat(checkWrite(Bytes.FLOAT), f);
    return this;
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    bytes.writeFloat(checkWrite(offset, Bytes.FLOAT), f);
    return this;
  }

  @Override
  public Buffer writeDouble(double d) {
    bytes.writeDouble(checkWrite(Bytes.DOUBLE), d);
    return this;
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    bytes.writeDouble(checkWrite(offset, Bytes.DOUBLE), d);
    return this;
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    bytes.writeBoolean(checkWrite(Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    bytes.writeBoolean(checkWrite(offset, Bytes.BYTE), b);
    return this;
  }

  @Override
  public Buffer writeString(String s) {
    if (s == null) {
      return writeByte(0);
    } else {
      byte[] bytes = s.getBytes();
      return writeByte(1)
        .writeUnsignedShort(bytes.length)
        .write(bytes, 0, bytes.length);
    }
  }

  @Override
  public Buffer writeString(long offset, String s) {
    if (s == null) {
      return writeByte(offset, 0);
    } else {
      byte[] bytes = s.getBytes();
      return writeByte(offset, 1)
        .writeUnsignedShort(offset + Bytes.BYTE, bytes.length)
        .write(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
    }
  }

  @Override
  public Buffer writeUTF8(String s) {
    if (s == null) {
      return writeByte(0);
    } else {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      return writeByte(1).writeUnsignedShort(bytes.length).write(bytes, 0, bytes.length);
    }
  }

  @Override
  public Buffer writeUTF8(long offset, String s) {
    if (s == null) {
      return writeByte(offset, 0);
    } else {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      return writeByte(offset, 1)
        .writeUnsignedShort(offset + Bytes.BYTE, bytes.length)
        .write(offset + Bytes.BYTE + Bytes.SHORT, bytes, 0, bytes.length);
    }
  }

  @Override
  public Buffer flush() {
    bytes.flush();
    return this;
  }

  @Override
  public void close() {
    references.set(0);
    if (referenceManager != null) {
      referenceManager.release(this);
    } else {
      bytes.close();
    }
  }

}
