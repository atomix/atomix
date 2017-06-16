/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.util.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Byte buffer bytes.
 */
public abstract class ByteBufferBytes extends AbstractBytes {
  protected ByteBuffer buffer;

  protected ByteBufferBytes(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public Bytes reset(ByteBuffer buffer) {
    buffer.clear();
    this.buffer = checkNotNull(buffer, "buffer cannot be null");
    return this;
  }

  /**
   * Allocates a new byte buffer.
   *
   * @param size the buffer size
   * @return a newly allocated byte buffer
   */
  protected abstract ByteBuffer newByteBuffer(long size);

  @Override
  public Bytes resize(long newSize) {
    ByteBuffer oldBuffer = buffer;
    ByteBuffer newBuffer = newByteBuffer(newSize);
    oldBuffer.position(0).limit(buffer.capacity());
    newBuffer.position(0).limit(buffer.capacity());
    newBuffer.put(oldBuffer);
    newBuffer.clear();
    return reset(newBuffer);
  }

  @Override
  public byte[] array() {
    return buffer.array();
  }

  /**
   * Returns the underlying {@link ByteBuffer}.
   *
   * @return the underlying byte buffer
   */
  public ByteBuffer byteBuffer() {
    return buffer;
  }

  @Override
  public Bytes zero() {
    return this;
  }

  @Override
  public long size() {
    return buffer.capacity();
  }

  @Override
  public ByteOrder order() {
    return buffer.order();
  }

  @Override
  public Bytes order(ByteOrder order) {
    return reset(buffer.order(order));
  }

  /**
   * Returns the index for the given offset.
   */
  private int index(long offset) {
    return (int) offset;
  }

  @Override
  public Bytes zero(long offset) {
    for (int i = index(offset); i < buffer.capacity(); i++) {
      buffer.put(i, (byte) 0);
    }
    return this;
  }

  @Override
  public Bytes zero(long offset, long length) {
    for (int i = index(offset); i < offset + length; i++) {
      buffer.put(i, (byte) 0);
    }
    return this;
  }

  @Override
  public Bytes read(long position, byte[] bytes, long offset, long length) {
    for (int i = 0; i < length; i++) {
      bytes[index(offset) + i] = (byte) readByte(position + i);
    }
    return this;
  }

  @Override
  public Bytes read(long position, Bytes bytes, long offset, long length) {
    for (int i = 0; i < length; i++) {
      bytes.writeByte(offset + i, readByte(position + i));
    }
    return this;
  }

  @Override
  public Bytes write(long position, byte[] bytes, long offset, long length) {
    for (int i = 0; i < length; i++) {
      buffer.put((int) position + i, (byte) bytes[index(offset) + i]);
    }
    return this;
  }

  @Override
  public Bytes write(long position, Bytes bytes, long offset, long length) {
    for (int i = 0; i < length; i++) {
      buffer.put((int) position + i, (byte) bytes.readByte(offset + i));
    }
    return this;
  }

  @Override
  public int readByte(long offset) {
    return buffer.get(index(offset));
  }

  @Override
  public char readChar(long offset) {
    return buffer.getChar(index(offset));
  }

  @Override
  public short readShort(long offset) {
    return buffer.getShort(index(offset));
  }

  @Override
  public int readInt(long offset) {
    return buffer.getInt(index(offset));
  }

  @Override
  public long readLong(long offset) {
    return buffer.getLong(index(offset));
  }

  @Override
  public float readFloat(long offset) {
    return buffer.getFloat(index(offset));
  }

  @Override
  public double readDouble(long offset) {
    return buffer.getDouble(index(offset));
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    buffer.put(index(offset), (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    buffer.putChar(index(offset), c);
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    buffer.putShort(index(offset), s);
    return this;
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    buffer.putInt(index(offset), i);
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    buffer.putLong(index(offset), l);
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    buffer.putFloat(index(offset), f);
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    buffer.putDouble(index(offset), d);
    return this;
  }

}
