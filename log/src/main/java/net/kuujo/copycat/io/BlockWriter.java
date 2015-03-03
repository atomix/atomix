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
 * Block writer.
 * <p>
 * The block writer exposes the write portion of the buffer interface.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BlockWriter extends BytesNavigator<BlockWriter> implements WritableBytes<BlockWriter>, AutoCloseable {
  private final Block block;

  public BlockWriter(Block block) {
    super(block.capacity());
    this.block = block;
  }

  @Override
  public BlockWriter write(byte[] bytes) {
    block.write(bytes, getAndSetPosition(checkWrite(position(), bytes.length)), bytes.length);
    return this;
  }

  @Override
  public BlockWriter write(byte[] bytes, long offset, int length) {
    checkWrite(offset, length);
    block.write(bytes, offset, length);
    return this;
  }

  @Override
  public BlockWriter writeByte(int b) {
    block.writeByte(getAndSetPosition(checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public BlockWriter writeByte(long offset, int b) {
    checkWrite(offset, Byte.BYTES);
    block.writeByte(offset, b);
    return this;
  }

  @Override
  public BlockWriter writeChar(char c) {
    block.writeChar(getAndSetPosition(checkWrite(position(), Character.BYTES)), c);
    return this;
  }

  @Override
  public BlockWriter writeChar(long offset, char c) {
    checkWrite(offset, Character.BYTES);
    block.writeChar(offset, c);
    return this;
  }

  @Override
  public BlockWriter writeShort(short s) {
    block.writeShort(getAndSetPosition(checkWrite(position(), Short.BYTES)), s);
    return this;
  }

  @Override
  public BlockWriter writeShort(long offset, short s) {
    checkWrite(offset, Short.BYTES);
    block.writeShort(offset, s);
    return this;
  }

  @Override
  public BlockWriter writeInt(int i) {
    block.writeInt(getAndSetPosition(checkWrite(position(), Integer.BYTES)), i);
    return this;
  }

  @Override
  public BlockWriter writeInt(long offset, int i) {
    checkWrite(offset, Integer.BYTES);
    block.writeInt(offset, i);
    return this;
  }

  @Override
  public BlockWriter writeLong(long l) {
    block.writeLong(getAndSetPosition(checkWrite(position(), Long.BYTES)), l);
    return this;
  }

  @Override
  public BlockWriter writeLong(long offset, long l) {
    checkWrite(offset, Long.BYTES);
    block.writeLong(offset, l);
    return this;
  }

  @Override
  public BlockWriter writeFloat(float f) {
    block.writeFloat(getAndSetPosition(checkWrite(position(), Float.BYTES)), f);
    return this;
  }

  @Override
  public BlockWriter writeFloat(long offset, float f) {
    checkWrite(offset, Float.BYTES);
    block.writeFloat(offset, f);
    return this;
  }

  @Override
  public BlockWriter writeDouble(double d) {
    block.writeDouble(getAndSetPosition(checkWrite(position(), Double.BYTES)), d);
    return this;
  }

  @Override
  public BlockWriter writeDouble(long offset, double d) {
    checkWrite(offset, Double.BYTES);
    block.writeDouble(offset, d);
    return this;
  }

  @Override
  public BlockWriter writeBoolean(boolean b) {
    block.writeBoolean(getAndSetPosition(checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public BlockWriter writeBoolean(long offset, boolean b) {
    checkWrite(offset, Byte.BYTES);
    block.writeBoolean(offset, b);
    return this;
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
