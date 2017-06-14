/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.buffer;

import java.nio.ByteOrder;

/**
 * Wrapped bytes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WrappedBytes extends AbstractBytes {
  protected final Bytes bytes;
  private final Bytes root;

  public WrappedBytes(Bytes bytes) {
    if (bytes == null)
      throw new NullPointerException("bytes cannot be null");
    this.bytes = bytes;
    this.root = bytes instanceof WrappedBytes ? ((WrappedBytes) bytes).root : bytes;
  }

  /**
   * Returns the root bytes.
   */
  public Bytes root() {
    return root;
  }

  @Override
  public long size() {
    return bytes.size();
  }

  @Override
  public Bytes resize(long newSize) {
    return bytes.resize(newSize);
  }

  @Override
  public ByteOrder order() {
    return bytes.order();
  }

  @Override
  public Bytes zero() {
    bytes.zero();
    return this;
  }

  @Override
  public Bytes zero(long offset) {
    bytes.zero(offset);
    return this;
  }

  @Override
  public Bytes zero(long offset, long length) {
    bytes.zero(offset, length);
    return this;
  }

  @Override
  public Bytes read(long offset, Bytes dst, long dstOffset, long length) {
    bytes.read(offset, dst, dstOffset, length);
    return this;
  }

  @Override
  public Bytes read(long offset, byte[] dst, long dstOffset, long length) {
    bytes.read(offset, dst, dstOffset, length);
    return this;
  }

  @Override
  public int readByte(long offset) {
    return bytes.readByte(offset);
  }

  @Override
  public int readUnsignedByte(long offset) {
    return bytes.readUnsignedByte(offset);
  }

  @Override
  public char readChar(long offset) {
    return bytes.readChar(offset);
  }

  @Override
  public short readShort(long offset) {
    return bytes.readShort(offset);
  }

  @Override
  public int readUnsignedShort(long offset) {
    return bytes.readUnsignedShort(offset);
  }

  @Override
  public int readMedium(long offset) {
    return bytes.readMedium(offset);
  }

  @Override
  public int readUnsignedMedium(long offset) {
    return bytes.readUnsignedMedium(offset);
  }

  @Override
  public int readInt(long offset) {
    return bytes.readInt(offset);
  }

  @Override
  public long readUnsignedInt(long offset) {
    return bytes.readUnsignedInt(offset);
  }

  @Override
  public long readLong(long offset) {
    return bytes.readLong(offset);
  }

  @Override
  public float readFloat(long offset) {
    return bytes.readFloat(offset);
  }

  @Override
  public double readDouble(long offset) {
    return bytes.readDouble(offset);
  }

  @Override
  public boolean readBoolean(long offset) {
    return bytes.readBoolean(offset);
  }

  @Override
  public String readString(long offset) {
    return bytes.readString(offset);
  }

  @Override
  public String readUTF8(long offset) {
    return bytes.readUTF8(offset);
  }

  @Override
  public Bytes write(long offset, Bytes src, long srcOffset, long length) {
    bytes.write(offset, src, srcOffset, length);
    return this;
  }

  @Override
  public Bytes write(long offset, byte[] src, long srcOffset, long length) {
    bytes.write(offset, src, srcOffset, length);
    return this;
  }

  @Override
  public Bytes writeByte(long offset, int b) {
    bytes.writeByte(offset, b);
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(long offset, int b) {
    bytes.writeUnsignedByte(offset, b);
    return this;
  }

  @Override
  public Bytes writeChar(long offset, char c) {
    bytes.writeChar(offset, c);
    return this;
  }

  @Override
  public Bytes writeShort(long offset, short s) {
    bytes.writeShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(long offset, int s) {
    bytes.writeUnsignedShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeMedium(long offset, int m) {
    bytes.writeMedium(offset, m);
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(long offset, int m) {
    bytes.writeUnsignedMedium(offset, m);
    return this;
  }

  @Override
  public Bytes writeInt(long offset, int i) {
    bytes.writeInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(long offset, long i) {
    bytes.writeUnsignedInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeLong(long offset, long l) {
    bytes.writeLong(offset, l);
    return this;
  }

  @Override
  public Bytes writeFloat(long offset, float f) {
    bytes.writeFloat(offset, f);
    return this;
  }

  @Override
  public Bytes writeDouble(long offset, double d) {
    bytes.writeDouble(offset, d);
    return this;
  }

  @Override
  public Bytes writeBoolean(long offset, boolean b) {
    bytes.writeBoolean(offset, b);
    return this;
  }

  @Override
  public Bytes writeString(long offset, String s) {
    bytes.writeString(offset, s);
    return this;
  }

  @Override
  public Bytes writeUTF8(long offset, String s) {
    bytes.writeUTF8(offset, s);
    return this;
  }

  @Override
  public Bytes flush() {
    bytes.flush();
    return this;
  }

  @Override
  public void close() {
    bytes.close();
  }

}
