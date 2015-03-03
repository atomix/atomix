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
 * Buffer reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BlockReader extends BytesNavigator<BlockReader> implements ReadableBytes<BlockReader>, AutoCloseable {
  private final Block block;

  public BlockReader(Block block) {
    super(block.capacity());
    this.block = block;
  }

  @Override
  public BlockReader read(byte[] bytes) {
    return read(bytes, getAndSetPosition(checkRead(position(), bytes.length)), bytes.length);
  }

  @Override
  public BlockReader read(byte[] bytes, long offset, int length) {
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
    return block.readShort(getAndSetPosition(checkRead(position(), Short.BYTES)));
  }

  @Override
  public short readShort(long offset) {
    checkRead(offset, Short.BYTES);
    return block.readShort(offset);
  }

  @Override
  public int readInt() {
    return block.readInt(getAndSetPosition(checkRead(position(), Integer.BYTES)));
  }

  @Override
  public int readInt(long offset) {
    checkRead(offset, Integer.BYTES);
    return block.readInt(offset);
  }

  @Override
  public long readLong() {
    return block.readLong(getAndSetPosition(checkRead(position(), Long.BYTES)));
  }

  @Override
  public long readLong(long offset) {
    checkRead(offset, Long.BYTES);
    return block.readLong(offset);
  }

  @Override
  public float readFloat() {
    return block.readFloat(getAndSetPosition(checkRead(position(), Float.BYTES)));
  }

  @Override
  public float readFloat(long offset) {
    checkRead(offset, Float.BYTES);
    return block.readFloat(offset);
  }

  @Override
  public double readDouble() {
    return block.readDouble(getAndSetPosition(checkRead(position(), Double.BYTES)));
  }

  @Override
  public double readDouble(long offset) {
    checkRead(offset, Double.BYTES);
    return block.readDouble(offset);
  }

  @Override
  public boolean readBoolean() {
    return block.readBoolean(getAndSetPosition(checkRead(position(), Byte.BYTES)));
  }

  @Override
  public boolean readBoolean(long offset) {
    checkRead(offset, Byte.BYTES);
    return block.readBoolean(offset);
  }

  @Override
  public void close() {
    // Do nothing useful.
  }

}
