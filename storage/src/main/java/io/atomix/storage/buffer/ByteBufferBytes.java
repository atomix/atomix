/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.storage.buffer;

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
  protected abstract ByteBuffer newByteBuffer(int size);

  @Override
  public Bytes resize(int newSize) {
    ByteBuffer oldBuffer = buffer;
    ByteBuffer newBuffer = newByteBuffer(newSize);
    oldBuffer.position(0).limit(oldBuffer.capacity());
    newBuffer.position(0).limit(newBuffer.capacity());
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
  public int size() {
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
  private int index(int offset) {
    return (int) offset;
  }

  @Override
  public Bytes zero(int offset) {
    for (int i = index(offset); i < buffer.capacity(); i++) {
      buffer.put(i, (byte) 0);
    }
    return this;
  }

  @Override
  public Bytes zero(int offset, int length) {
    for (int i = index(offset); i < offset + length; i++) {
      buffer.put(i, (byte) 0);
    }
    return this;
  }

  @Override
  public Bytes read(int position, byte[] bytes, int offset, int length) {
    for (int i = 0; i < length; i++) {
      bytes[index(offset) + i] = (byte) readByte(position + i);
    }
    return this;
  }

  @Override
  public Bytes read(int position, Bytes bytes, int offset, int length) {
    for (int i = 0; i < length; i++) {
      bytes.writeByte(offset + i, readByte(position + i));
    }
    return this;
  }

  @Override
  public Bytes write(int position, byte[] bytes, int offset, int length) {
    for (int i = 0; i < length; i++) {
      buffer.put((int) position + i, (byte) bytes[index(offset) + i]);
    }
    return this;
  }

  @Override
  public Bytes write(int position, Bytes bytes, int offset, int length) {
    for (int i = 0; i < length; i++) {
      buffer.put((int) position + i, (byte) bytes.readByte(offset + i));
    }
    return this;
  }

  @Override
  public int readByte(int offset) {
    return buffer.get(index(offset));
  }

  @Override
  public char readChar(int offset) {
    return buffer.getChar(index(offset));
  }

  @Override
  public short readShort(int offset) {
    return buffer.getShort(index(offset));
  }

  @Override
  public int readInt(int offset) {
    return buffer.getInt(index(offset));
  }

  @Override
  public long readLong(int offset) {
    return buffer.getLong(index(offset));
  }

  @Override
  public float readFloat(int offset) {
    return buffer.getFloat(index(offset));
  }

  @Override
  public double readDouble(int offset) {
    return buffer.getDouble(index(offset));
  }

  @Override
  public Bytes writeByte(int offset, int b) {
    buffer.put(index(offset), (byte) b);
    return this;
  }

  @Override
  public Bytes writeChar(int offset, char c) {
    buffer.putChar(index(offset), c);
    return this;
  }

  @Override
  public Bytes writeShort(int offset, short s) {
    buffer.putShort(index(offset), s);
    return this;
  }

  @Override
  public Bytes writeInt(int offset, int i) {
    buffer.putInt(index(offset), i);
    return this;
  }

  @Override
  public Bytes writeLong(int offset, long l) {
    buffer.putLong(index(offset), l);
    return this;
  }

  @Override
  public Bytes writeFloat(int offset, float f) {
    buffer.putFloat(index(offset), f);
    return this;
  }

  @Override
  public Bytes writeDouble(int offset, double d) {
    buffer.putDouble(index(offset), d);
    return this;
  }

}
