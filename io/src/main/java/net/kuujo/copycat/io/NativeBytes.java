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
import net.kuujo.copycat.io.util.NativeMemory;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Native bytes.
 * <p>
 * Bytes are read from and written to the JVM's underlying static {@link sun.misc.Unsafe} instance. Bytes are read in
 * {@link java.nio.ByteOrder#nativeOrder()} order and if necessary bytes are reversed to {@link java.nio.ByteOrder#BIG_ENDIAN}
 * order.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeBytes extends AbstractBytes {

  /**
   * Allocates a new native byte array.
   * <p>
   * When the array is constructed, {@link net.kuujo.copycat.io.util.NativeMemoryAllocator} will be used to allocate
   * {@code size} bytes of off-heap memory. Memory is accessed by the buffer directly via {@link sun.misc.Unsafe}.
   *
   * @param size The size of the buffer to allocate (in bytes).
   * @return The native buffer.
   */
  public static NativeBytes allocate(long size) {
    return new NativeBytes(NativeMemory.allocate(size));
  }

  private static final boolean NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
  protected NativeMemory memory;

  protected NativeBytes(NativeMemory memory) {
    this.memory = memory;
  }

  /**
   * Copies the bytes to a new byte array.
   *
   * @return A new {@link NativeBytes} instance backed by a copy of this instance's memory.
   */
  public NativeBytes copy() {
    return new NativeBytes(memory.copy());
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
  public boolean isDirect() {
    return true;
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
    memory.unsafe().setMemory(memory.address(offset), length, (byte) 0);
    return this;
  }

  @Override
  public Bytes read(long position, Bytes bytes, long offset, long length) {
    checkRead(position, length);

    if (bytes instanceof WrappedBytes)
      bytes = ((WrappedBytes) bytes).root();

    if (bytes instanceof NativeBytes) {
      memory.unsafe().copyMemory(memory.address(position), ((NativeBytes) bytes).memory.address(), length);
    } else if (bytes instanceof HeapBytes) {
      memory.unsafe().copyMemory(null, memory.address(position), ((HeapBytes) bytes).memory.array(), ((HeapBytes) bytes).memory.address(offset), length);
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
    memory.unsafe().copyMemory(null, memory.address(position), bytes, HeapMemory.ARRAY_BASE_OFFSET + offset, length);
    return this;
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Byte.BYTES);
    return memory.getByte(offset);
  }

  @Override
  public int readUnsignedByte(long offset) {
    return Byte.toUnsignedInt((byte) readByte(offset));
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Character.BYTES);
    return NATIVE_ORDER ? memory.getChar(offset) : Character.reverseBytes(memory.getChar(offset));
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Short.BYTES);
    return NATIVE_ORDER ? memory.getShort(offset) : Short.reverseBytes(memory.getShort(offset));
  }

  @Override
  public int readUnsignedShort(long offset) {
    return Short.toUnsignedInt(readShort(offset));
  }

  @Override
  public int readMedium(long offset) {
    checkRead(offset, 3);
    return NATIVE_ORDER
      ? (memory.getByte(offset)) << 16
      | (memory.getByte(offset + 1) & 0xff) << 8
      | (memory.getByte(offset + 2) & 0xff)
      : (memory.getByte(offset + 2)) << 16
      | (memory.getByte(offset + 1) & 0xff) << 8
      | (memory.getByte(offset) & 0xff);
  }

  @Override
  public int readUnsignedMedium(long offset) {
    checkRead(offset, 3);
    return NATIVE_ORDER
      ? (memory.getByte(offset) & 0xff) << 16
        | (memory.getByte(offset + 1) & 0xff) << 8
        | (memory.getByte(offset + 2) & 0xff)
      : (memory.getByte(offset + 2) & 0xff) << 16
        | (memory.getByte(offset + 1) & 0xff) << 8
        | (memory.getByte(offset) & 0xff);
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Integer.BYTES);
    return NATIVE_ORDER ? memory.getInt(offset) : Integer.reverseBytes(memory.getInt(offset));
  }

  @Override
  public long readUnsignedInt(long offset) {
    return Integer.toUnsignedLong(readInt(offset));
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Long.BYTES);
    return NATIVE_ORDER ? memory.getLong(offset) : Long.reverseBytes(memory.getLong(offset));
  }

  @Override
  public float readFloat(long offset) {
    return Float.intBitsToFloat(readInt(offset));
  }

  @Override
  public double readDouble(long offset) {
    return Double.longBitsToDouble(readLong(offset));
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Byte.BYTES);
    return memory.getByte(offset) == (byte) 1;
  }

  @Override
  public String readUTF8(long offset) {
    byte[] bytes = new byte[readUnsignedShort(offset)];
    read(offset + Short.BYTES, bytes, 0, bytes.length);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public Bytes write(long position, Bytes bytes, long offset, long length) {
    checkWrite(position, length);
    if (bytes.size() < length)
      throw new IllegalArgumentException("length is greater than provided byte array size");

    if (bytes instanceof WrappedBytes)
      bytes = ((WrappedBytes) bytes).root();

    if (bytes instanceof NativeBytes) {
      memory.unsafe().copyMemory(((NativeBytes) bytes).memory.address(offset), memory.address(position), length);
    } else if (bytes instanceof HeapBytes) {
      memory.unsafe().copyMemory(((HeapBytes) bytes).memory.array(), ((HeapBytes) bytes).memory.address(offset), null, memory.address(position), length);
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
    memory.unsafe().copyMemory(bytes, HeapMemory.ARRAY_BASE_OFFSET + offset, null, memory.address(position), length);
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    memory.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    checkWrite(offset, Character.BYTES);
    memory.putChar(offset, NATIVE_ORDER ? c : Character.reverseBytes(c));
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    checkWrite(offset, Short.BYTES);
    memory.putShort(offset, NATIVE_ORDER ? s : Short.reverseBytes(s));
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(long offset, int s) {
    return writeShort(offset, (short) s);
  }

  @Override
  public Bytes writeMedium(long offset, int m) {
    if (NATIVE_ORDER) {
      memory.putByte(offset, (byte) (m >>> 16));
      memory.putByte(offset + 1, (byte) (m >>> 8));
      memory.putByte(offset + 2, (byte) m);
    } else {
      memory.putByte(offset + 2, (byte) (m >>> 16));
      memory.putByte(offset + 1, (byte) (m >>> 8));
      memory.putByte(offset, (byte) m);
    }
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(long offset, int m) {
    return writeMedium(offset, m);
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    checkWrite(offset, Integer.BYTES);
    memory.putInt(offset, NATIVE_ORDER ? i : Integer.reverseBytes(i));
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(long offset, long i) {
    return writeInt(offset, (int) i);
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    checkWrite(offset, Long.BYTES);
    memory.putLong(offset, NATIVE_ORDER ? l : Long.reverseBytes(l));
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    return writeInt(offset, Float.floatToRawIntBits(f));
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    return writeLong(offset, Double.doubleToRawLongBits(d));
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    return writeByte(offset, b ? (byte) 1 : (byte) 0);
  }

  @Override
  public Bytes writeUTF8(long offset, String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    return writeUnsignedShort(offset, bytes.length)
      .write(offset + Short.BYTES, bytes, 0, bytes.length);
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
