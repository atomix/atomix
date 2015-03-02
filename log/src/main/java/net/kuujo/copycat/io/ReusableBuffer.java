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
 * Buffer implementation that delegates reference counting to the underlying buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBuffer<T extends Buffer & Referenceable<?>> extends BufferNavigator<Buffer> implements Buffer {
  private final T buffer;

  public ReusableBuffer(T buffer) {
    super(buffer);
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
    buffer.acquire();
  }

  T buffer() {
    return buffer;
  }

  @Override
  public char readChar() {
    return buffer.readChar(checkBounds(incrementPosition(Character.BYTES)));
  }

  @Override
  public char readChar(long offset) {
    return buffer.readChar(checkBounds(offset, Character.BYTES));
  }

  @Override
  public short readShort() {
    return buffer.readShort(checkBounds(incrementPosition(Short.BYTES)));
  }

  @Override
  public short readShort(long offset) {
    return buffer.readShort(checkBounds(offset, Short.BYTES));
  }

  @Override
  public int readInt() {
    return buffer.readInt(checkBounds(incrementPosition(Integer.BYTES)));
  }

  @Override
  public int readInt(long offset) {
    return buffer.readInt(checkBounds(offset, Integer.BYTES));
  }

  @Override
  public long readLong() {
    return buffer.readLong(checkBounds(incrementPosition(Long.BYTES)));
  }

  @Override
  public long readLong(long offset) {
    return buffer.readLong(checkBounds(offset, Long.BYTES));
  }

  @Override
  public float readFloat() {
    return buffer.readFloat(checkBounds(incrementPosition(Float.BYTES)));
  }

  @Override
  public float readFloat(long offset) {
    return buffer.readFloat(checkBounds(offset, Float.BYTES));
  }

  @Override
  public double readDouble() {
    return buffer.readDouble(checkBounds(incrementPosition(Double.BYTES)));
  }

  @Override
  public double readDouble(long offset) {
    return buffer.readDouble(checkBounds(offset, Double.BYTES));
  }

  @Override
  public boolean readBoolean() {
    return buffer.readBoolean(checkBounds(incrementPosition(1)));
  }

  @Override
  public boolean readBoolean(long offset) {
    return buffer.readBoolean(checkBounds(offset, 1));
  }

  @Override
  public Buffer writeChar(char c) {
    buffer.writeChar(checkBounds(incrementPosition(Character.BYTES)), c);
    return this;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    buffer.writeChar(checkBounds(offset, Character.BYTES), c);
    return this;
  }

  @Override
  public Buffer writeShort(short s) {
    buffer.writeShort(checkBounds(incrementPosition(Short.BYTES)), s);
    return this;
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    buffer.writeShort(checkBounds(offset, Short.BYTES), s);
    return this;
  }

  @Override
  public Buffer writeInt(int i) {
    buffer.writeInt(checkBounds(incrementPosition(Integer.BYTES)), i);
    return this;
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    buffer.writeInt(checkBounds(offset, Integer.BYTES), i);
    return this;
  }

  @Override
  public Buffer writeLong(long l) {
    buffer.writeLong(checkBounds(incrementPosition(Long.BYTES)), l);
    return this;
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    buffer.writeLong(checkBounds(offset, Long.BYTES), l);
    return this;
  }

  @Override
  public Buffer writeFloat(float f) {
    buffer.writeFloat(checkBounds(incrementPosition(Float.BYTES)), f);
    return this;
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    buffer.writeFloat(checkBounds(offset, Float.BYTES), f);
    return this;
  }

  @Override
  public Buffer writeDouble(double d) {
    buffer.writeDouble(checkBounds(incrementPosition(Double.BYTES)), d);
    return this;
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    buffer.writeDouble(checkBounds(offset, Double.BYTES), d);
    return this;
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    buffer.writeBoolean(checkBounds(incrementPosition(1)), b);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    buffer.writeBoolean(checkBounds(offset, 1), b);
    return this;
  }

  @Override
  public void close() throws Exception {
    buffer.release();
  }

}
