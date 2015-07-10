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
package net.kuujo.copycat.cluster;

import io.netty.buffer.ByteBuf;
import net.kuujo.alleycat.io.Buffer;
import net.kuujo.alleycat.io.Bytes;

import java.nio.ByteOrder;
import java.nio.InvalidMarkException;
import java.nio.charset.StandardCharsets;

/**
 * Netty ByteBuf buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ByteBufBuffer implements Buffer {
  ByteBuf buffer;
  private long offset;
  private long position;
  private long limit = -1;
  private long mark = -1;

  ByteBufBuffer() {
  }

  ByteBufBuffer(ByteBuf buffer) {
    this.buffer = buffer;
  }

  void setByteBuf(ByteBuf buffer) {
    this.buffer = buffer;
    offset = 0;
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public boolean isFile() {
    return false;
  }

  @Override
  public ByteOrder order() {
    return buffer.order();
  }

  @Override
  public Buffer order(ByteOrder order) {
    return new ByteBufBuffer(buffer.order(order));
  }

  @Override
  public Buffer asReadOnlyBuffer() {
    return this;
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public long capacity() {
    return buffer.capacity();
  }

  @Override
  public Buffer capacity(long capacity) {
    buffer.capacity((int) capacity);
    return this;
  }

  @Override
  public long maxCapacity() {
    return buffer.maxCapacity();
  }

  @Override
  public Buffer compact() {
    buffer = buffer.slice();
    return this;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public Buffer position(long position) {
    if (limit != -1 && position > limit) {
      throw new IllegalArgumentException("position cannot be greater than limit");
    } else if (limit == -1 && position > buffer.maxCapacity()) {
      throw new IllegalArgumentException("position cannot be greater than capacity");
    }
    this.position = position;
    return this;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  public Buffer limit(long limit) {
    if (limit > buffer.maxCapacity())
      throw new IllegalArgumentException("limit cannot be greater than buffer capacity");
    if (limit < -1)
      throw new IllegalArgumentException("limit cannot be negative");
    this.limit = limit;
    return this;
  }

  @Override
  public long remaining() {
    return (limit == -1 ? buffer.maxCapacity() : limit) - position;
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public Buffer flip() {
    limit = position;
    position = 0;
    mark = -1;
    return this;
  }

  @Override
  public Buffer mark() {
    this.mark = position;
    return this;
  }

  @Override
  public Buffer rewind() {
    position = 0;
    mark = -1;
    return this;
  }

  @Override
  public Buffer reset() {
    if (mark == -1)
      throw new InvalidMarkException();
    position = mark;
    return this;
  }

  @Override
  public Buffer skip(long length) {
    if (length > remaining())
      throw new IndexOutOfBoundsException("length cannot be greater than remaining bytes in the buffer");
    position += length;
    return this;
  }

  @Override
  public Buffer clear() {
    position = 0;
    limit = -1;
    mark = -1;
    return this;
  }

  @Override
  public Bytes bytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Buffer slice() {
    return new ByteBufBuffer(buffer.slice());
  }

  @Override
  public Buffer slice(long length) {
    return new ByteBufBuffer(buffer.slice((int) position, (int) length));
  }

  @Override
  public Buffer slice(long offset, long length) {
    return new ByteBufBuffer(buffer.slice((int) offset, (int) length));
  }

  @Override
  public Buffer read(Buffer buffer) {
    byte[] bytes = new byte[this.buffer.readableBytes()];
    this.buffer.readBytes(bytes);
    buffer.write(bytes);
    return this;
  }

  @Override
  public Buffer read(Bytes bytes) {
    byte[] b = new byte[Math.min((int) bytes.size(), buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(0, b, 0, b.length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes) {
    buffer.readBytes(bytes);
    return this;
  }

  @Override
  public Buffer read(Bytes bytes, long dstOffset, long length) {
    byte[] b = new byte[Math.min((int) length, buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(dstOffset, b, 0, b.length);
    return this;
  }

  @Override
  public Buffer read(long srcOffset, Bytes bytes, long dstOffset, long length) {
    byte[] b = new byte[Math.min((int) length, buffer.writerIndex() - (int) srcOffset)];
    buffer.getBytes((int) srcOffset, b);
    bytes.write(dstOffset, b, 0, b.length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes, long offset, long length) {
    buffer.readBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public Buffer read(long srcOffset, byte[] bytes, long dstOffset, long length) {
    buffer.getBytes((int) srcOffset, bytes, (int) dstOffset, (int) length);
    return this;
  }

  @Override
  public int readByte() {
    return buffer.readByte();
  }

  @Override
  public int readByte(long offset) {
    return buffer.getByte((int) offset);
  }

  @Override
  public int readUnsignedByte() {
    return buffer.readUnsignedByte();
  }

  @Override
  public int readUnsignedByte(long offset) {
    return buffer.getUnsignedByte((int) offset);
  }

  @Override
  public char readChar() {
    return buffer.readChar();
  }

  @Override
  public char readChar(long offset) {
    return buffer.getChar((int) offset);
  }

  @Override
  public short readShort() {
    return buffer.readShort();
  }

  @Override
  public short readShort(long offset) {
    return buffer.getShort((int) offset);
  }

  @Override
  public int readUnsignedShort() {
    return buffer.readUnsignedShort();
  }

  @Override
  public int readUnsignedShort(long offset) {
    return buffer.getUnsignedShort((int) offset);
  }

  @Override
  public int readInt() {
    return buffer.readInt();
  }

  @Override
  public int readInt(long offset) {
    return buffer.getInt((int) offset);
  }

  @Override
  public long readUnsignedInt() {
    return buffer.readUnsignedInt();
  }

  @Override
  public long readUnsignedInt(long offset) {
    return buffer.getUnsignedInt((int) offset);
  }

  @Override
  public int readMedium() {
    return buffer.readMedium();
  }

  @Override
  public int readMedium(long offset) {
    return buffer.getMedium((int) offset);
  }

  @Override
  public int readUnsignedMedium() {
    return buffer.readUnsignedMedium();
  }

  @Override
  public int readUnsignedMedium(long offset) {
    return buffer.getUnsignedMedium((int) offset);
  }

  @Override
  public long readLong() {
    return buffer.readLong();
  }

  @Override
  public long readLong(long offset) {
    return buffer.getLong((int) offset);
  }

  @Override
  public float readFloat() {
    return buffer.readFloat();
  }

  @Override
  public float readFloat(long offset) {
    return buffer.getFloat((int) offset);
  }

  @Override
  public double readDouble() {
    return buffer.readDouble();
  }

  @Override
  public double readDouble(long offset) {
    return buffer.getDouble((int) offset);
  }

  @Override
  public boolean readBoolean() {
    return buffer.readBoolean();
  }

  @Override
  public boolean readBoolean(long offset) {
    return buffer.getBoolean((int) offset);
  }

  @Override
  public String readString() {
    return readUTF8();
  }

  @Override
  public String readString(long offset) {
    return readUTF8(offset);
  }

  @Override
  public String readUTF8() {
    byte[] bytes = new byte[buffer.readUnsignedShort()];
    buffer.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public String readUTF8(long offset) {
    byte[] bytes = new byte[buffer.getUnsignedShort((int) offset)];
    buffer.getBytes((int) offset + Short.BYTES, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public Buffer write(Buffer buffer) {
    byte[] bytes = new byte[Math.min((int) buffer.remaining(), this.buffer.writableBytes())];
    buffer.read(bytes);
    this.buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public Buffer write(Bytes bytes) {
    byte[] b = new byte[Math.min((int) bytes.size(), buffer.writableBytes())];
    bytes.read(0, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes) {
    buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public Buffer write(Bytes bytes, long offset, long length) {
    byte[] b = new byte[Math.min((int) bytes.size(), (int) length)];
    bytes.read(offset, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public Buffer write(long offset, Bytes bytes, long srcOffset, long length) {
    byte[] b = new byte[Math.min((int) bytes.size(), (int) length)];
    bytes.read(offset, b, 0, b.length);
    buffer.setBytes((int) offset, b);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes, long offset, long length) {
    buffer.writeBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public Buffer write(long offset, byte[] src, long srcOffset, long length) {
    buffer.setBytes((int) offset, src, (int) srcOffset, (int) length);
    return this;
  }

  @Override
  public Buffer writeByte(int b) {
    buffer.writeByte(b);
    return this;
  }

  @Override
  public Buffer writeByte(long offset, int b) {
    buffer.setByte((int) offset, b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(int b) {
    buffer.writeByte(b);
    return this;
  }

  @Override
  public Buffer writeUnsignedByte(long offset, int b) {
    buffer.setByte((int) offset, b);
    return this;
  }

  @Override
  public Buffer writeChar(char c) {
    buffer.writeChar(c);
    return this;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    buffer.setChar((int) offset, c);
    return this;
  }

  @Override
  public Buffer writeShort(short s) {
    buffer.writeShort(s);
    return this;
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    buffer.setShort((int) offset, s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(int s) {
    buffer.writeShort(s);
    return this;
  }

  @Override
  public Buffer writeUnsignedShort(long offset, int s) {
    buffer.setShort((int) offset, s);
    return this;
  }

  @Override
  public Buffer writeInt(int i) {
    buffer.writeInt(i);
    return this;
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    buffer.setInt((int) offset, i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long i) {
    buffer.writeInt((int) i);
    return this;
  }

  @Override
  public Buffer writeUnsignedInt(long offset, long i) {
    buffer.setInt((int) offset, (int) i);
    return this;
  }

  @Override
  public Buffer writeMedium(int m) {
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public Buffer writeMedium(long offset, int m) {
    buffer.setMedium((int) offset, m);
    return this;
  }

  @Override
  public Buffer writeUnsignedMedium(int m) {
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public Buffer writeUnsignedMedium(long offset, int m) {
    buffer.setMedium((int) offset, m);
    return this;
  }

  @Override
  public Buffer writeLong(long l) {
    buffer.writeLong(l);
    return this;
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    buffer.setLong((int) offset, l);
    return this;
  }

  @Override
  public Buffer writeFloat(float f) {
    buffer.writeFloat(f);
    return this;
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    buffer.setFloat((int) offset, f);
    return this;
  }

  @Override
  public Buffer writeDouble(double d) {
    buffer.writeDouble(d);
    return this;
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    buffer.setDouble((int) offset, d);
    return this;
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    buffer.writeBoolean(b);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    buffer.setBoolean((int) offset, b);
    return this;
  }

  @Override
  public Buffer writeString(String s) {
    return writeUTF8(s);
  }

  @Override
  public Buffer writeString(long offset, String s) {
    return writeUTF8(offset, s);
  }

  @Override
  public Buffer writeUTF8(String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    buffer.writeShort(bytes.length).writeBytes(bytes);
    return this;
  }

  @Override
  public Buffer writeUTF8(long offset, String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    buffer.setShort((int) offset, bytes.length).setBytes((int) offset + Short.BYTES, bytes);
    return this;
  }

  @Override
  public Buffer zero() {
    return this;
  }

  @Override
  public Buffer zero(long offset) {
    return this;
  }

  @Override
  public Buffer zero(long offset, long length) {
    return this;
  }

  @Override
  public Buffer flush() {
    return this;
  }

  @Override
  public Buffer acquire() {
    return this;
  }

  @Override
  public void release() {

  }

  @Override
  public int references() {
    return 0;
  }

  @Override
  public void close() {

  }
}
