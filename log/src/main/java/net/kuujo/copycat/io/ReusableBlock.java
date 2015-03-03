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

import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.io.util.Referenceable;

/**
 * Reusable block.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBlock<T extends Block & Referenceable<?>> extends BytesNavigator<Block> implements Block {
  private final T block;
  private final ReferenceManager<ReusableBlock<T>> referenceManager;

  public ReusableBlock(T block, ReferenceManager<ReusableBlock<T>> referenceManager) {
    super(block.capacity());
    this.block = block;
    this.referenceManager = referenceManager;
  }

  /**
   * Returns the underlying block.
   */
  T block() {
    return block;
  }

  @Override
  public int index() {
    return block.index();
  }

  @Override
  public BlockReader reader() {
    return block.reader();
  }

  @Override
  public BlockWriter writer() {
    return block.writer();
  }

  @Override
  public Block read(byte[] bytes) {
    block.read(bytes, getAndSetPosition(checkRead(position(), bytes.length)), bytes.length);
    return this;
  }

  @Override
  public Block read(byte[] bytes, long offset, int length) {
    checkRead(offset, length);
    block.read(bytes, offset, length);
    return this;
  }

  @Override
  public int readByte() {
    return block.readByte(getAndSetPosition(checkRead(position(), Byte.BYTES)));
  }

  @Override
  public int readByte(long offset) {
    checkRead(offset, Byte.BYTES);
    return block.readByte(offset);
  }

  @Override
  public char readChar() {
    return block.readChar(getAndSetPosition(checkRead(position(), Character.BYTES)));
  }

  @Override
  public char readChar(long offset) {
    checkRead(offset, Character.BYTES);
    return block.readChar(offset);
  }

  @Override
  public short readShort() {
    checkRead(position(), Short.BYTES);
    return block.readShort(getAndSetPosition(checkRead(position(), Short.BYTES)));
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Short.BYTES);
    return block.readShort(offset);
  }

  @Override
  public int readInt() {
    checkRead(position(), Integer.BYTES);
    return block.readInt(getAndSetPosition(checkRead(position(), Integer.BYTES)));
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Integer.BYTES);
    return block.readInt(offset);
  }

  @Override
  public long readLong() {
    checkRead(position(), Long.BYTES);
    return block.readLong(getAndSetPosition(checkRead(position(), Long.BYTES)));
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Long.BYTES);
    return block.readLong(offset);
  }

  @Override
  public float readFloat() {
    checkRead(position(), Float.BYTES);
    return block.readFloat(getAndSetPosition(checkRead(position(), Float.BYTES)));
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Float.BYTES);
    return block.readFloat(offset);
  }

  @Override
  public double readDouble() {
    checkRead(position(), Double.BYTES);
    return block.readDouble(getAndSetPosition(checkRead(position(), Double.BYTES)));
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Double.BYTES);
    return block.readDouble(offset);
  }

  @Override
  public boolean readBoolean() {
    checkRead(position(), Byte.BYTES);
    return block.readBoolean(getAndSetPosition(checkRead(position(), Byte.BYTES)));
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(position(), Byte.BYTES);
    return block.readBoolean(offset);
  }

  @Override
  public Block write(byte[] bytes) {
    block.write(bytes, getAndSetPosition(checkWrite(position(), bytes.length)), bytes.length);
    return this;
  }

  @Override
  public Block write(byte[] bytes, long offset, int length) {
    checkWrite(offset, length);
    block.write(bytes, offset, length);
    return this;
  }

  @Override
  public Block writeByte(int b) {
    block.writeByte(getAndSetPosition(checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public Block writeByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    block.writeByte(offset, b);
    return this;
  }

  @Override
  public Block writeChar(char c) {
    block.writeChar(getAndSetPosition(checkWrite(position(), Character.BYTES)), c);
    return this;
  }

  @Override
  public Block writeChar(long offset, char c) {
    checkWrite(offset, Character.BYTES);
    block.writeChar(offset, c);
    return this;
  }

  @Override
  public Block writeShort(short s) {
    block.writeShort(getAndSetPosition(checkWrite(position(), Short.BYTES)), s);
    return this;
  }

  @Override
  public Block writeShort(long offset, short s) {
    checkWrite(offset, Short.BYTES);
    block.writeShort(offset, s);
    return this;
  }

  @Override
  public Block writeInt(int i) {
    block.writeInt(getAndSetPosition(checkWrite(position(), Integer.BYTES)), i);
    return this;
  }

  @Override
  public Block writeInt(long offset, int i) {
    checkWrite(offset, Integer.BYTES);
    block.writeInt(offset, i);
    return this;
  }

  @Override
  public Block writeLong(long l) {
    block.writeLong(getAndSetPosition(checkWrite(position(), Long.BYTES)), l);
    return this;
  }

  @Override
  public Block writeLong(long offset, long l) {
    checkWrite(offset, Long.BYTES);
    block.writeLong(offset, l);
    return this;
  }

  @Override
  public Block writeFloat(float f) {
    block.writeFloat(getAndSetPosition(checkWrite(position(), Float.BYTES)), f);
    return this;
  }

  @Override
  public Block writeFloat(long offset, float f) {
    checkWrite(offset, Float.BYTES);
    block.writeFloat(offset, f);
    return this;
  }

  @Override
  public Block writeDouble(double d) {
    block.writeDouble(getAndSetPosition(checkWrite(position(), Double.BYTES)), d);
    return this;
  }

  @Override
  public Block writeDouble(long offset, double d) {
    checkWrite(offset, Double.BYTES);
    block.writeDouble(offset, d);
    return this;
  }

  @Override
  public Block writeBoolean(boolean b) {
    block.writeBoolean(getAndSetPosition(checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public Block writeBoolean(long offset, boolean b) {
    checkWrite(offset, Byte.BYTES);
    block.writeBoolean(offset, b);
    return this;
  }

  @Override
  public void close() throws Exception {
    block.release();
  }

}
