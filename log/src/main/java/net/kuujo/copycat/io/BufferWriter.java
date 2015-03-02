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
 * Buffer writer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferWriter extends BufferNavigator<BufferWriter> implements WritableBuffer<BufferWriter>, AutoCloseable {
  private final Buffer buffer;

  public BufferWriter(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  @Override
  public BufferWriter writeChar(char c) {
    buffer.writeChar(checkBounds(incrementPosition(Character.BYTES)), c);
    return this;
  }

  @Override
  public BufferWriter writeChar(long offset, char c) {
    buffer.writeChar(checkBounds(offset, Character.BYTES), c);
    return this;
  }

  @Override
  public BufferWriter writeShort(short s) {
    buffer.writeShort(checkBounds(incrementPosition(Short.BYTES)), s);
    return this;
  }

  @Override
  public BufferWriter writeShort(long offset, short s) {
    buffer.writeShort(checkBounds(offset, Short.BYTES), s);
    return this;
  }

  @Override
  public BufferWriter writeInt(int i) {
    buffer.writeInt(checkBounds(incrementPosition(Integer.BYTES)), i);
    return this;
  }

  @Override
  public BufferWriter writeInt(long offset, int i) {
    buffer.writeInt(checkBounds(offset, Integer.BYTES), i);
    return this;
  }

  @Override
  public BufferWriter writeLong(long l) {
    buffer.writeLong(checkBounds(incrementPosition(Long.BYTES)), l);
    return this;
  }

  @Override
  public BufferWriter writeLong(long offset, long l) {
    buffer.writeLong(checkBounds(offset, Long.BYTES), l);
    return this;
  }

  @Override
  public BufferWriter writeFloat(float f) {
    buffer.writeFloat(checkBounds(incrementPosition(Float.BYTES)), f);
    return this;
  }

  @Override
  public BufferWriter writeFloat(long offset, float f) {
    buffer.writeFloat(checkBounds(offset, Float.BYTES), f);
    return this;
  }

  @Override
  public BufferWriter writeDouble(double d) {
    buffer.writeDouble(checkBounds(incrementPosition(Double.BYTES)), d);
    return this;
  }

  @Override
  public BufferWriter writeDouble(long offset, double d) {
    buffer.writeDouble(checkBounds(offset, Double.BYTES), d);
    return this;
  }

  @Override
  public BufferWriter writeBoolean(boolean b) {
    buffer.writeBoolean(checkBounds(incrementPosition(1)), b);
    return this;
  }

  @Override
  public BufferWriter writeBoolean(long offset, boolean b) {
    buffer.writeBoolean(checkBounds(offset, 1), b);
    return this;
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
