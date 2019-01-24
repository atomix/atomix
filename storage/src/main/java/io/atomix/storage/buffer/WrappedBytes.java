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
    if (bytes == null) {
      throw new NullPointerException("bytes cannot be null");
    }
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
  public int size() {
    return bytes.size();
  }

  @Override
  public Bytes resize(int newSize) {
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
  public Bytes zero(int offset) {
    bytes.zero(offset);
    return this;
  }

  @Override
  public Bytes zero(int offset, int length) {
    bytes.zero(offset, length);
    return this;
  }

  @Override
  public Bytes read(int offset, Bytes dst, int dstOffset, int length) {
    bytes.read(offset, dst, dstOffset, length);
    return this;
  }

  @Override
  public Bytes read(int offset, byte[] dst, int dstOffset, int length) {
    bytes.read(offset, dst, dstOffset, length);
    return this;
  }

  @Override
  public int readByte(int offset) {
    return bytes.readByte(offset);
  }

  @Override
  public int readUnsignedByte(int offset) {
    return bytes.readUnsignedByte(offset);
  }

  @Override
  public char readChar(int offset) {
    return bytes.readChar(offset);
  }

  @Override
  public short readShort(int offset) {
    return bytes.readShort(offset);
  }

  @Override
  public int readUnsignedShort(int offset) {
    return bytes.readUnsignedShort(offset);
  }

  @Override
  public int readMedium(int offset) {
    return bytes.readMedium(offset);
  }

  @Override
  public int readUnsignedMedium(int offset) {
    return bytes.readUnsignedMedium(offset);
  }

  @Override
  public int readInt(int offset) {
    return bytes.readInt(offset);
  }

  @Override
  public long readUnsignedInt(int offset) {
    return bytes.readUnsignedInt(offset);
  }

  @Override
  public long readLong(int offset) {
    return bytes.readLong(offset);
  }

  @Override
  public float readFloat(int offset) {
    return bytes.readFloat(offset);
  }

  @Override
  public double readDouble(int offset) {
    return bytes.readDouble(offset);
  }

  @Override
  public boolean readBoolean(int offset) {
    return bytes.readBoolean(offset);
  }

  @Override
  public String readString(int offset) {
    return bytes.readString(offset);
  }

  @Override
  public String readUTF8(int offset) {
    return bytes.readUTF8(offset);
  }

  @Override
  public Bytes write(int offset, Bytes src, int srcOffset, int length) {
    bytes.write(offset, src, srcOffset, length);
    return this;
  }

  @Override
  public Bytes write(int offset, byte[] src, int srcOffset, int length) {
    bytes.write(offset, src, srcOffset, length);
    return this;
  }

  @Override
  public Bytes writeByte(int offset, int b) {
    bytes.writeByte(offset, b);
    return this;
  }

  @Override
  public Bytes writeUnsignedByte(int offset, int b) {
    bytes.writeUnsignedByte(offset, b);
    return this;
  }

  @Override
  public Bytes writeChar(int offset, char c) {
    bytes.writeChar(offset, c);
    return this;
  }

  @Override
  public Bytes writeShort(int offset, short s) {
    bytes.writeShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(int offset, int s) {
    bytes.writeUnsignedShort(offset, s);
    return this;
  }

  @Override
  public Bytes writeMedium(int offset, int m) {
    bytes.writeMedium(offset, m);
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(int offset, int m) {
    bytes.writeUnsignedMedium(offset, m);
    return this;
  }

  @Override
  public Bytes writeInt(int offset, int i) {
    bytes.writeInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(int offset, long i) {
    bytes.writeUnsignedInt(offset, i);
    return this;
  }

  @Override
  public Bytes writeLong(int offset, long l) {
    bytes.writeLong(offset, l);
    return this;
  }

  @Override
  public Bytes writeFloat(int offset, float f) {
    bytes.writeFloat(offset, f);
    return this;
  }

  @Override
  public Bytes writeDouble(int offset, double d) {
    bytes.writeDouble(offset, d);
    return this;
  }

  @Override
  public Bytes writeBoolean(int offset, boolean b) {
    bytes.writeBoolean(offset, b);
    return this;
  }

  @Override
  public Bytes writeString(int offset, String s) {
    bytes.writeString(offset, s);
    return this;
  }

  @Override
  public Bytes writeUTF8(int offset, String s) {
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
