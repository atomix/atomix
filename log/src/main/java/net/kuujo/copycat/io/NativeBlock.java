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
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Native memory block.
 * <p>
 * Given a {@link Memory} descriptor the native block implementation accesses native memory directly via
 * {@link sun.misc.Unsafe}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeBlock extends AbstractBlock<NativeBlock> {
  private final Memory memory;

  NativeBlock(int index, Memory memory, ReferenceManager<NativeBlock> manager) {
    super(index, memory.size(), manager);
    this.memory = memory;
  }

  /**
   * Returns the block memory descriptor.
   *
   * @return The block memory descriptor.
   */
  Memory memory() {
    return memory;
  }

  /**
   * Returns the address for the given offset.
   */
  private long address(long offset) {
    return memory.address() + offset;
  }

  @Override
  public Block read(byte[] bytes) {
    checkRead(position(), bytes.length);
    long position = position();
    long address = address(position);
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = NativeMemory.UNSAFE.getByte(address + i);
      position(position + i);
    }
    return this;
  }

  @Override
  public Block read(byte[] bytes, long offset, int length) {
    checkRead(position(), length);
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      bytes[i] = NativeMemory.UNSAFE.getByte(address + i);
    }
    return this;
  }

  @Override
  public int readByte() {
    return NativeMemory.UNSAFE.getByte(address(getAndSetPosition(checkRead(position(), Byte.BYTES))));
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Byte.BYTES);
    return NativeMemory.UNSAFE.getByte(address(offset));
  }

  @Override
  public char readChar() {
    return NativeMemory.UNSAFE.getChar(address(getAndSetPosition(checkRead(position(), Character.BYTES))));
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Character.BYTES);
    return NativeMemory.UNSAFE.getChar(address(offset));
  }

  @Override
  public short readShort() {
    return NativeMemory.UNSAFE.getShort(address(getAndSetPosition(checkRead(position(), Short.BYTES))));
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Short.BYTES);
    return NativeMemory.UNSAFE.getShort(address(offset));
  }

  @Override
  public int readInt() {
    return NativeMemory.UNSAFE.getInt(address(getAndSetPosition(checkRead(position(), Integer.BYTES))));
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Integer.BYTES);
    return NativeMemory.UNSAFE.getInt(address(offset));
  }

  @Override
  public long readLong() {
    return NativeMemory.UNSAFE.getLong(address(getAndSetPosition(checkRead(position(), Long.BYTES))));
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Long.BYTES);
    return NativeMemory.UNSAFE.getLong(address(offset));
  }

  @Override
  public float readFloat() {
    return NativeMemory.UNSAFE.getFloat(address(getAndSetPosition(checkRead(position(), Float.BYTES))));
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Float.BYTES);
    return NativeMemory.UNSAFE.getFloat(address(offset));
  }

  @Override
  public double readDouble() {
    return NativeMemory.UNSAFE.getDouble(address(getAndSetPosition(checkRead(position(), Double.BYTES))));
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Double.BYTES);
    return NativeMemory.UNSAFE.getDouble(address(offset));
  }

  @Override
  public boolean readBoolean() {
    return NativeMemory.UNSAFE.getByte(address(getAndSetPosition(checkRead(position(), Byte.BYTES)))) == (byte) 1;
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Byte.BYTES);
    return NativeMemory.UNSAFE.getByte(address(offset)) == (byte) 1;
  }

  @Override
  public Block write(byte[] bytes) {
    checkWrite(position(), bytes.length);
    long position = position();
    long address = address(position);
    for (int i = 0; i < bytes.length; i++) {
      NativeMemory.UNSAFE.putByte(address + i, bytes[i]);
      position(position + i);
    }
    return this;
  }

  @Override
  public Block write(byte[] bytes, long offset, int length) {
    checkWrite(offset, length);
    if (bytes.length < length)
      throw new IllegalArgumentException("length is greater than provided byte array length");
    long address = address(offset);
    for (int i = 0; i < length; i++) {
      NativeMemory.UNSAFE.putByte(address + i, bytes[i]);
    }
    return this;
  }

  @Override
  public Block writeByte(int b) {
    NativeMemory.UNSAFE.putByte(getAndSetPosition(checkWrite(position(), Byte.BYTES)), (byte) b);
    return this;
  }

  @Override
  public Block writeByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    NativeMemory.UNSAFE.putByte(offset, (byte) b);
    return this;
  }

  @Override
  public Block writeChar(char c) {
    NativeMemory.UNSAFE.putChar(getAndSetPosition(checkWrite(position(), Character.BYTES)), c);
    return this;
  }

  @Override
  public Block writeChar(long offset, char c) {
    checkWrite(offset, Character.BYTES);
    NativeMemory.UNSAFE.putChar(address(offset), c);
    return this;
  }

  @Override
  public Block writeShort(short s) {
    NativeMemory.UNSAFE.putShort(getAndSetPosition(checkWrite(position(), Character.BYTES)), s);
    return this;
  }

  @Override
  public NativeBlock writeShort(long offset, short s) {
    checkWrite(offset, Short.BYTES);
    NativeMemory.UNSAFE.putShort(address(offset), s);
    return this;
  }

  @Override
  public Block writeInt(int i) {
    NativeMemory.UNSAFE.putInt(getAndSetPosition(checkWrite(position(), Integer.BYTES)), i);
    return this;
  }

  @Override
  public NativeBlock writeInt(long offset, int i) {
    checkWrite(offset, Integer.BYTES);
    NativeMemory.UNSAFE.putInt(address(offset), i);
    return this;
  }

  @Override
  public Block writeLong(long l) {
    NativeMemory.UNSAFE.putLong(getAndSetPosition(checkWrite(position(), Long.BYTES)), l);
    return this;
  }

  @Override
  public NativeBlock writeLong(long offset, long l) {
    checkWrite(offset, Long.BYTES);
    NativeMemory.UNSAFE.putLong(address(offset), l);
    return this;
  }

  @Override
  public Block writeFloat(float f) {
    NativeMemory.UNSAFE.putFloat(getAndSetPosition(checkWrite(position(), Float.BYTES)), f);
    return this;
  }

  @Override
  public NativeBlock writeFloat(long offset, float f) {
    checkWrite(offset, Float.BYTES);
    NativeMemory.UNSAFE.putFloat(address(offset), f);
    return this;
  }

  @Override
  public Block writeDouble(double d) {
    NativeMemory.UNSAFE.putDouble(getAndSetPosition(checkWrite(position(), Double.BYTES)), d);
    return this;
  }

  @Override
  public NativeBlock writeDouble(long offset, double d) {
    checkWrite(offset, Double.BYTES);
    NativeMemory.UNSAFE.putDouble(address(offset), d);
    return this;
  }

  @Override
  public Block writeBoolean(boolean b) {
    NativeMemory.UNSAFE.putByte(getAndSetPosition(checkWrite(position(), Byte.BYTES)), b ? (byte) 1 : (byte) 0);
    return this;
  }

  @Override
  public Block writeBoolean(long offset, boolean b) {
    checkWrite(offset, Byte.BYTES);
    NativeMemory.UNSAFE.putByte(address(offset), b ? (byte) 1 : (byte) 0);
    return this;
  }

}
