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

/**
 * Native memory block.
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
  public char readChar(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getChar(address(offset));
  }

  @Override
  public short readShort(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getShort(address(offset));
  }

  @Override
  public int readInt(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getInt(address(offset));
  }

  @Override
  public long readLong(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getLong(address(offset));
  }

  @Override
  public float readFloat(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getFloat(address(offset));
  }

  @Override
  public double readDouble(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getDouble(address(offset));
  }

  @Override
  public boolean readBoolean(long offset) {
    checkBounds(offset);
    return NativeMemory.UNSAFE.getByte(address(offset)) == (byte) 1;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putChar(address(offset), c);
    return this;
  }

  @Override
  public NativeBlock writeShort(long offset, short s) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putShort(address(offset), s);
    return this;
  }

  @Override
  public NativeBlock writeInt(long offset, int i) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putInt(address(offset), i);
    return this;
  }

  @Override
  public NativeBlock writeLong(long offset, long l) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putLong(address(offset), l);
    return this;
  }

  @Override
  public NativeBlock writeFloat(long offset, float f) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putFloat(address(offset), f);
    return this;
  }

  @Override
  public NativeBlock writeDouble(long offset, double d) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putDouble(address(offset), d);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    checkBounds(offset);
    NativeMemory.UNSAFE.putByte(address(offset), b ? (byte) 1 : (byte) 0);
    return this;
  }

}
