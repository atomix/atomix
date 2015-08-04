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

import net.kuujo.copycat.io.util.HeapMemory;

import java.nio.charset.StandardCharsets;

/**
 * Java heap bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapBytes extends AbstractBytes {

  /**
   * Allocates a new heap byte array.
   * <p>
   * When the array is constructed, {@link net.kuujo.copycat.io.util.HeapMemoryAllocator} will be used to allocate
   * {@code count} bytes on the Java heap.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *         an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static HeapBytes allocate(long size) {
    if (size > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("size cannot for HeapBytes cannot be greater than " + HeapMemory.MAX_SIZE);
    return new HeapBytes(HeapMemory.allocate(size));
  }

  /**
   * Wraps the given bytes in a {@link net.kuujo.copycat.io.HeapBytes} object.
   * <p>
   * The returned {@link Bytes} object will be backed by a {@link net.kuujo.copycat.io.util.HeapMemory} instance that
   * wraps the given byte array. The {@link net.kuujo.copycat.io.Bytes#size()} will be equivalent to the provided
   * by array {@code length}.
   *
   * @param bytes The bytes to wrap.
   */
  public static HeapBytes wrap(byte[] bytes) {
    return new HeapBytes(HeapMemory.wrap(bytes));
  }

  protected HeapMemory memory;

  protected HeapBytes(HeapMemory memory) {
    this.memory = memory;
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link HeapBytes} instance backed by a copy of this instance's array.
   */
  public HeapBytes copy() {
    return new HeapBytes(memory.copy());
  }

  /**
   * Returns the underlying byte array.
   *
   * @return The underlying byte array.
   */
  public byte[] array() {
    return memory.array();
  }

  /**
   * Resets the heap byte array.
   *
   * @param array The internal byte array.
   * @return The heap bytes.
   */
  public HeapBytes reset(byte[] array) {
    memory.reset(array);
    return this;
  }

  @Override
  public long size() {
    return memory.size();
  }

  @Override
  public Bytes resize(long newSize) {
    this.memory = memory.allocator().reallocate(memory, newSize);
    return this;
  }

  @Override
  public Bytes zero() {
    return zero(0, memory.size());
  }

  @Override
  public Bytes zero(long offset) {
    return zero(offset, memory.size() - offset);
  }

  @Override
  public Bytes zero(long offset, long length) {
    memory.unsafe().setMemory(memory.array(), memory.address(offset), length, (byte) 0);
    return this;
  }

  @Override
  public Bytes read(long position, Bytes bytes, long offset, long length) {
    checkRead(position, length);
    if (bytes instanceof HeapBytes) {
      memory.unsafe().copyMemory(memory.array(), memory.address(position), ((HeapBytes) bytes).memory.array(), ((HeapBytes) bytes).memory.address(offset), length);
    } else if (bytes instanceof NativeBytes) {
      memory.unsafe().copyMemory(memory.array(), memory.address(position), null, ((NativeBytes) bytes).memory.address(offset), length);
    } else {
      for (int i = 0; i < length; i++) {
        bytes.writeByte(offset + i, memory.getByte(position + i));
      }
    }
    return this;
  }

  @Override
  public Bytes read(long position, byte[] bytes, long offset, long length) {
    checkRead(position, length);
    memory.unsafe().copyMemory(memory.array(), memory.address(position), bytes, memory.address(offset), length);
    return this;
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Bytes.BYTE);
    return memory.getByte(offset);
  }

  @Override
  public int readUnsignedByte(long offset) {
    checkRead(offset, Bytes.BYTE);
    return memory.getByte(offset) & 0xFF;
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Bytes.CHARACTER);
    return memory.getChar(offset);
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Bytes.SHORT);
    return memory.getShort(offset);
  }

  @Override
  public int readUnsignedShort(long offset) {
    checkRead(offset, Bytes.SHORT);
    return memory.getShort(offset) & 0xFFFF;
  }

  @Override
  public int readMedium(long offset) {
    checkRead(offset, Bytes.MEDIUM);
    return (memory.getByte(offset)) << 16
      | (memory.getByte(offset + 1) & 0xff) << 8
      | (memory.getByte(offset + 2) & 0xff);
  }

  @Override
  public int readUnsignedMedium(long offset) {
    checkRead(offset, Bytes.MEDIUM);
    return (memory.getByte(offset) & 0xff) << 16
      | (memory.getByte(offset + 1) & 0xff) << 8
      | (memory.getByte(offset + 2) & 0xff);
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Bytes.INTEGER);
    return memory.getInt(offset);
  }

  @Override
  public long readUnsignedInt(long offset) {
    checkRead(offset, Bytes.INTEGER);
    return memory.getInt(offset) & 0xFFFFFFFFL;
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Bytes.LONG);
    return memory.getLong(offset);
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Bytes.FLOAT);
    return memory.getFloat(offset);
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Bytes.DOUBLE);
    return memory.getDouble(offset);
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Bytes.BOOLEAN);
    return memory.getByte(offset) == (byte) 1;
  }

  @Override
  public String readString(long offset) {
    byte[] bytes = new byte[readUnsignedShort(offset)];
    read(offset + Bytes.SHORT, bytes, 0, bytes.length);
    return new String(bytes);
  }

  @Override
  public String readUTF8(long offset) {
    byte[] bytes = new byte[readUnsignedShort(offset)];
    read(offset + Bytes.SHORT, bytes, 0, bytes.length);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public Bytes write(long position, Bytes bytes, long offset, long length) {
    checkWrite(position, length);
    if (bytes.size() < length)
      throw new IllegalArgumentException("length is greater than provided byte array size");

    if (bytes instanceof HeapBytes) {
      memory.unsafe().copyMemory(((HeapBytes) bytes).memory.array(), ((HeapBytes) bytes).memory.address(offset), memory.array(), memory.address(position), length);
    } else if (bytes instanceof NativeBytes) {
      memory.unsafe().copyMemory(null, ((NativeBytes) bytes).memory.address(offset), memory.array(), memory.address(position), length);
    } else {
      for (int i = 0; i < length; i++) {
        memory.putByte(position + i, (byte) bytes.readByte(offset + i));
      }
    }
    return this;
  }

  @Override
  public Bytes write(long position, byte[] bytes, long offset, long length) {
    checkWrite(position, length);
    if (bytes.length < length)
      throw new IllegalArgumentException("length is greater than provided byte array length");
    memory.unsafe().copyMemory(bytes, memory.address(offset), memory.array(), memory.address(position), length);
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    checkWrite(offset, Bytes.BYTE);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(long offset, int b) {
    checkWrite(offset, Bytes.BYTE);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    checkWrite(offset, Bytes.CHARACTER);
    memory.putChar(offset, c);
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    checkWrite(offset, Bytes.SHORT);
    memory.putShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(long offset, int s) {
    checkWrite(offset, Bytes.SHORT);
    memory.putShort(offset, (short) s);
    return this;
  }

  @Override
  public Bytes writeMedium(long offset, int m) {
    memory.putByte(offset, (byte) (m >>> 16));
    memory.putByte(offset + 1, (byte) (m >>> 8));
    memory.putByte(offset + 2, (byte) m);
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(long offset, int m) {
    return writeMedium(offset, m);
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    checkWrite(offset, Bytes.INTEGER);
    memory.putInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(long offset, long i) {
    checkWrite(offset, Bytes.INTEGER);
    memory.putInt(offset, (int) i);
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    checkWrite(offset, Bytes.LONG);
    memory.putLong(offset, l);
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    checkWrite(offset, Bytes.FLOAT);
    memory.putFloat(offset, f);
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    checkWrite(offset, Bytes.DOUBLE);
    memory.putDouble(offset, d);
    return this;
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    checkWrite(offset, Bytes.BOOLEAN);
    memory.putByte(offset, b ? (byte) 1 : (byte) 0);
    return this;
  }

  @Override
  public Bytes writeString(long offset, String s) {
    byte[] bytes = s.getBytes();
    return writeUnsignedShort(offset, bytes.length)
      .write(offset + Bytes.SHORT, bytes, 0, bytes.length);
  }

  @Override
  public Bytes writeUTF8(long offset, String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    return writeUnsignedShort(offset, bytes.length)
      .write(offset + Bytes.SHORT, bytes, 0, bytes.length);
  }

  @Override
  public Bytes flush() {
    return this;
  }

  @Override
  public void close() {
    flush();
    super.close();
  }
}
