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
 * Bytes in swapped order.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SwappedBytes extends WrappedBytes {

  public SwappedBytes(Bytes bytes) {
    super(bytes);
  }

  @Override
  public ByteOrder order() {
    return bytes.order() == ByteOrder.BIG_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  }

  @Override
  public char readChar(int offset) {
    return Character.reverseBytes(bytes.readChar(offset));
  }

  @Override
  public short readShort(int offset) {
    return Short.reverseBytes(bytes.readShort(offset));
  }

  @Override
  public int readUnsignedShort(int offset) {
    return Short.reverseBytes(bytes.readShort(offset)) & 0xFFFF;
  }

  @Override
  public int readMedium(int offset) {
    return Integer.reverseBytes(bytes.readMedium(offset));
  }

  @Override
  public int readUnsignedMedium(int offset) {
    return Integer.reverseBytes(bytes.readUnsignedMedium(offset));
  }

  @Override
  public int readInt(int offset) {
    return Integer.reverseBytes(bytes.readInt(offset));
  }

  @Override
  public long readUnsignedInt(int offset) {
    return Integer.reverseBytes(bytes.readInt(offset)) & 0xFFFFFFFFL;
  }

  @Override
  public long readLong(int offset) {
    return Long.reverseBytes(bytes.readLong(offset));
  }

  @Override
  public float readFloat(int offset) {
    return Float.intBitsToFloat(readInt(offset));
  }

  @Override
  public double readDouble(int offset) {
    return Double.longBitsToDouble(readLong(offset));
  }

  @Override
  public Bytes writeChar(int offset, char c) {
    bytes.writeChar(offset, Character.reverseBytes(c));
    return this;
  }

  @Override
  public Bytes writeShort(int offset, short s) {
    bytes.writeShort(offset, Short.reverseBytes(s));
    return this;
  }

  @Override
  public Bytes writeUnsignedShort(int offset, int s) {
    bytes.writeUnsignedShort(offset, Short.reverseBytes((short) s));
    return this;
  }

  @Override
  public Bytes writeMedium(int offset, int m) {
    bytes.writeMedium(offset, Integer.reverseBytes(m));
    return this;
  }

  @Override
  public Bytes writeUnsignedMedium(int offset, int m) {
    bytes.writeUnsignedMedium(offset, Integer.reverseBytes(m));
    return this;
  }

  @Override
  public Bytes writeInt(int offset, int i) {
    bytes.writeInt(offset, Integer.reverseBytes(i));
    return this;
  }

  @Override
  public Bytes writeUnsignedInt(int offset, long i) {
    bytes.writeUnsignedInt(offset, Integer.reverseBytes((int) i));
    return this;
  }

  @Override
  public Bytes writeLong(int offset, long l) {
    bytes.writeLong(offset, Long.reverseBytes(l));
    return this;
  }

  @Override
  public Bytes writeFloat(int offset, float f) {
    return writeInt(offset, Float.floatToRawIntBits(f));
  }

  @Override
  public Bytes writeDouble(int offset, double d) {
    return writeLong(offset, Double.doubleToRawLongBits(d));
  }

}
