/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.storage.buffer;

import io.atomix.utils.memory.HeapMemory;

/**
 * Java heap bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeHeapBytes extends AbstractBytes {

  /**
   * Allocates a new heap byte array.
   * <p>
   * When the array is constructed, {@link io.atomix.utils.memory.HeapMemoryAllocator} will be used to allocate
   * {@code count} bytes on the Java heap.
   *
   * @param size The count of the buffer to allocate (in bytes).
   * @return The heap buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
   *                                  an array on the Java heap - {@code Integer.MAX_VALUE - 5}
   */
  public static UnsafeHeapBytes allocate(int size) {
    if (size > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("size cannot for HeapBytes cannot be greater than " + HeapMemory.MAX_SIZE);
    return new UnsafeHeapBytes(HeapMemory.allocate(size));
  }

  /**
   * Wraps the given bytes in a {@link UnsafeHeapBytes} object.
   * <p>
   * The returned {@link Bytes} object will be backed by a {@link HeapMemory} instance that
   * wraps the given byte array. The {@link Bytes#size()} will be equivalent to the provided
   * by array {@code length}.
   *
   * @param bytes The bytes to wrap.
   */
  public static UnsafeHeapBytes wrap(byte[] bytes) {
    return new UnsafeHeapBytes(HeapMemory.wrap(bytes));
  }

  protected HeapMemory memory;

  protected UnsafeHeapBytes(HeapMemory memory) {
    this.memory = memory;
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link UnsafeHeapBytes} instance backed by a copy of this instance's array.
   */
  public UnsafeHeapBytes copy() {
    return new UnsafeHeapBytes(memory.copy());
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public byte[] array() {
    return memory.array();
  }

  /**
   * Resets the heap byte array.
   *
   * @param array The internal byte array.
   * @return The heap bytes.
   */
  public UnsafeHeapBytes reset(byte[] array) {
    memory.reset(array);
    return this;
  }

  @Override
  public int size() {
    return memory.size();
  }

  @Override
  public Bytes resize(int newSize) {
    this.memory = memory.allocator().reallocate(memory, newSize);
    return this;
  }

  @Override
  public Bytes zero() {
    return zero(0, memory.size());
  }

  @Override
  public Bytes zero(int offset) {
    return zero(offset, memory.size() - offset);
  }

  @Override
  public Bytes zero(int offset, int length) {
    memory.unsafe().setMemory(memory.array(), memory.address(offset), length, (byte) 0);
    return this;
  }

  @Override
  public Bytes read(int position, Bytes bytes, int offset, int length) {
    checkRead(position, length);
    if (bytes instanceof UnsafeHeapBytes) {
      memory.unsafe().copyMemory(memory.array(), memory.address(position), ((UnsafeHeapBytes) bytes).memory.array(), ((UnsafeHeapBytes) bytes).memory.address(offset), length);
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
  public Bytes read(int position, byte[] bytes, int offset, int length) {
    checkRead(position, length);
    memory.unsafe().copyMemory(memory.array(), memory.address(position), bytes, memory.address(offset), length);
    return this;
  }

  @Override
  public int readByte(int offset) {
    checkRead(offset, BYTE);
    return memory.getByte(offset);
  }

  @Override
  public int readUnsignedByte(int offset) {
    checkRead(offset, BYTE);
    return memory.getByte(offset) & 0xFF;
  }

  @Override
  public char readChar(int offset) {
    checkRead(offset, CHARACTER);
    return memory.getChar(offset);
  }

  @Override
  public short readShort(int offset) {
    checkRead(offset, SHORT);
    return memory.getShort(offset);
  }

  @Override
  public int readUnsignedShort(int offset) {
    checkRead(offset, SHORT);
    return memory.getShort(offset) & 0xFFFF;
  }

  @Override
  public int readMedium(int offset) {
    checkRead(offset, MEDIUM);
    return (memory.getByte(offset)) << 16
        | (memory.getByte(offset + 1) & 0xff) << 8
        | (memory.getByte(offset + 2) & 0xff);
  }

  @Override
  public int readUnsignedMedium(int offset) {
    checkRead(offset, MEDIUM);
    return (memory.getByte(offset) & 0xff) << 16
        | (memory.getByte(offset + 1) & 0xff) << 8
        | (memory.getByte(offset + 2) & 0xff);
  }

  @Override
  public int readInt(int offset) {
    checkRead(offset, INTEGER);
    return memory.getInt(offset);
  }

  @Override
  public long readUnsignedInt(int offset) {
    checkRead(offset, INTEGER);
    return memory.getInt(offset) & 0xFFFFFFFFL;
  }

  @Override
  public long readLong(int offset) {
    checkRead(offset, LONG);
    return memory.getLong(offset);
  }

  @Override
  public float readFloat(int offset) {
    checkRead(offset, FLOAT);
    return memory.getFloat(offset);
  }

  @Override
  public double readDouble(int offset) {
    checkRead(offset, DOUBLE);
    return memory.getDouble(offset);
  }

  @Override
  public boolean readBoolean(int offset) {
    checkRead(offset, BOOLEAN);
    return memory.getByte(offset) == (byte) 1;
  }

  @Override
  public Bytes write(int position, Bytes bytes, int offset, int length) {
    checkWrite(position, length);
    if (bytes.size() < length)
      throw new IllegalArgumentException("length is greater than provided byte array size");

    if (bytes instanceof UnsafeHeapBytes) {
      memory.unsafe().copyMemory(((UnsafeHeapBytes) bytes).memory.array(), ((UnsafeHeapBytes) bytes).memory.address(offset), memory.array(), memory.address(position), length);
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
  public Bytes write(int position, byte[] bytes, int offset, int length) {
    checkWrite(position, length);
    if (bytes.length < length)
      throw new IllegalArgumentException("length is greater than provided byte array length");
    memory.unsafe().copyMemory(bytes, memory.address(offset), memory.array(), memory.address(position), length);
    return this;
  }

  @Override
  public Bytes writeByte(int offset, int b) {
    checkWrite(offset, BYTE);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(int offset, int b) {
    checkWrite(offset, BYTE);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(int offset, char c) {
    checkWrite(offset, CHARACTER);
    memory.putChar(offset, c);
    return this;
  }

  @Override
  public Bytes writeShort(int offset, short s) {
    checkWrite(offset, SHORT);
    memory.putShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(int offset, int s) {
    checkWrite(offset, SHORT);
    memory.putShort(offset, (short) s);
    return this;
  }

  @Override
  public Bytes writeMedium(int offset, int m) {
    memory.putByte(offset, (byte) (m >>> 16));
    memory.putByte(offset + 1, (byte) (m >>> 8));
    memory.putByte(offset + 2, (byte) m);
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(int offset, int m) {
    return writeMedium(offset, m);
  }

  @Override
  public Bytes writeInt(int offset, int i) {
    checkWrite(offset, INTEGER);
    memory.putInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(int offset, long i) {
    checkWrite(offset, INTEGER);
    memory.putInt(offset, (int) i);
    return this;
  }

  @Override
  public Bytes writeLong(int offset, long l) {
    checkWrite(offset, LONG);
    memory.putLong(offset, l);
    return this;
  }

  @Override
  public Bytes writeFloat(int offset, float f) {
    checkWrite(offset, FLOAT);
    memory.putFloat(offset, f);
    return this;
  }

  @Override
  public Bytes writeDouble(int offset, double d) {
    checkWrite(offset, DOUBLE);
    memory.putDouble(offset, d);
    return this;
  }

  @Override
  public Bytes writeBoolean(int offset, boolean b) {
    checkWrite(offset, BOOLEAN);
    memory.putByte(offset, b ? (byte) 1 : (byte) 0);
    return this;
  }

}
