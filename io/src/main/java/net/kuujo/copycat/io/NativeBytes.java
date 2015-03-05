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
import net.kuujo.copycat.io.util.NativeMemory;

/**
 * Native bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeBytes implements Bytes {
  private final Memory memory;
  private final BufferNavigator navigator;

  public NativeBytes(Memory memory) {
    if (memory == null)
      throw new NullPointerException("memory cannot be null");
    this.memory = memory;
    this.navigator = new BufferNavigator(0, memory.size());
  }

  /**
   * Returns the memory descriptor for the bytes.
   */
  Memory memory() {
    return memory;
  }

  @Override
  public long size() {
    return memory.size();
  }

  /**
   * Returns the memory address for the given offset.
   */
  private long address(long offset) {
    return memory.address() + offset;
  }

  @Override
  public Bytes read(Bytes bytes, long offset, long length) {
    navigator.checkRead(offset, length);
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      bytes.writeByte(i, NativeMemory.UNSAFE.getByte(address + i));
    }
    return this;
  }

  @Override
  public Bytes read(byte[] bytes, long offset, long length) {
    navigator.checkRead(offset, length);
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      bytes[i] = NativeMemory.UNSAFE.getByte(address + i);
    }
    return this;
  }

  @Override
  public int readByte(long offset) {
    navigator.checkRead(offset, Byte.BYTES);
    return NativeMemory.UNSAFE.getByte(address(offset));
  }

  @Override
  public char readChar(long offset) {
    navigator.checkRead(offset, Character.BYTES);
    return NativeMemory.UNSAFE.getChar(address(offset));
  }

  @Override
  public short readShort(long offset) {
    navigator.checkRead(offset, Short.BYTES);
    return NativeMemory.UNSAFE.getShort(address(offset));
  }

  @Override
  public int readInt(long offset) {
    navigator.checkRead(offset, Integer.BYTES);
    return NativeMemory.UNSAFE.getInt(address(offset));
  }

  @Override
  public long readLong(long offset) {
    navigator.checkRead(offset, Long.BYTES);
    return NativeMemory.UNSAFE.getLong(address(offset));
  }

  @Override
  public float readFloat(long offset) {
    navigator.checkRead(offset, Float.BYTES);
    return NativeMemory.UNSAFE.getFloat(address(offset));
  }

  @Override
  public double readDouble(long offset) {
    navigator.checkRead(offset, Double.BYTES);
    return NativeMemory.UNSAFE.getDouble(address(offset));
  }

  @Override
  public boolean readBoolean(long offset) {
    navigator.checkRead(offset, Byte.BYTES);
    return NativeMemory.UNSAFE.getByte(address(offset)) == (byte) 1;
  }

  @Override
  public Bytes write(Bytes bytes, long offset, long length) {
    navigator.checkWrite(offset, length);
    if (bytes.size() < length)
      throw new IllegalArgumentException("length is greater than provided byte array size");
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      NativeMemory.UNSAFE.putByte(address + i, (byte) bytes.readByte(i));
    }
    return this;
  }

  @Override
  public Bytes write(byte[] bytes, long offset, long length) {
    navigator.checkWrite(offset, length);
    if (bytes.length < length)
      throw new IllegalArgumentException("length is greater than provided byte array length");
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      NativeMemory.UNSAFE.putByte(address + i, bytes[i]);
    }
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    navigator.checkWrite(offset, Byte.BYTES);
    NativeMemory.UNSAFE.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    navigator.checkWrite(offset, Character.BYTES);
    NativeMemory.UNSAFE.putChar(address(offset), c);
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    navigator.checkWrite(offset, Short.BYTES);
    NativeMemory.UNSAFE.putShort(address(offset), s);
    return this;
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    navigator.checkWrite(offset, Integer.BYTES);
    NativeMemory.UNSAFE.putInt(address(offset), i);
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    navigator.checkWrite(offset, Long.BYTES);
    NativeMemory.UNSAFE.putLong(address(offset), l);
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    navigator.checkWrite(offset, Float.BYTES);
    NativeMemory.UNSAFE.putFloat(address(offset), f);
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    navigator.checkWrite(offset, Double.BYTES);
    NativeMemory.UNSAFE.putDouble(address(offset), d);
    return this;
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    navigator.checkWrite(offset, Byte.BYTES);
    NativeMemory.UNSAFE.putByte(address(offset), b ? (byte) 1 : (byte) 0);
    return this;
  }

}
