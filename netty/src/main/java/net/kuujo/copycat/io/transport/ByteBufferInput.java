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
package net.kuujo.copycat.io.transport;

import io.netty.buffer.ByteBuf;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.Bytes;

import java.nio.charset.StandardCharsets;

/**
 * Byte buffer input.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ByteBufferInput implements BufferInput<ByteBufferInput> {
  ByteBuf buffer;

  /**
   * Sets the underlying byte buffer.
   */
  ByteBufferInput setByteBuf(ByteBuf buffer) {
    this.buffer = buffer;
    return this;
  }

  @Override
  public long remaining() {
    return buffer.readableBytes();
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public ByteBufferInput skip(long bytes) {
    buffer.readerIndex(buffer.readerIndex() + (int) bytes);
    return this;
  }

  @Override
  public ByteBufferInput read(Buffer buffer) {
    byte[] bytes = new byte[this.buffer.readableBytes()];
    this.buffer.readBytes(bytes);
    buffer.write(bytes);
    return this;
  }

  @Override
  public ByteBufferInput read(Bytes bytes) {
    byte[] b = new byte[Math.min((int) bytes.size(), buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(0, b, 0, b.length);
    return this;
  }

  @Override
  public ByteBufferInput read(byte[] bytes) {
    buffer.readBytes(bytes);
    return this;
  }

  @Override
  public ByteBufferInput read(Bytes bytes, long dstOffset, long length) {
    byte[] b = new byte[Math.min((int) length, buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(dstOffset, b, 0, b.length);
    return this;
  }

  @Override
  public ByteBufferInput read(byte[] bytes, long offset, long length) {
    buffer.readBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public int readByte() {
    return buffer.readByte();
  }

  @Override
  public int readUnsignedByte() {
    return buffer.readUnsignedByte();
  }

  @Override
  public char readChar() {
    return buffer.readChar();
  }

  @Override
  public short readShort() {
    return buffer.readShort();
  }

  @Override
  public int readUnsignedShort() {
    return buffer.readUnsignedShort();
  }

  @Override
  public int readInt() {
    return buffer.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return buffer.readUnsignedInt();
  }

  @Override
  public int readMedium() {
    return buffer.readMedium();
  }

  @Override
  public int readUnsignedMedium() {
    return buffer.readUnsignedMedium();
  }

  @Override
  public long readLong() {
    return buffer.readLong();
  }

  @Override
  public float readFloat() {
    return buffer.readFloat();
  }

  @Override
  public double readDouble() {
    return buffer.readDouble();
  }

  @Override
  public boolean readBoolean() {
    return buffer.readBoolean();
  }

  @Override
  public String readString() {
    return readUTF8();
  }

  @Override
  public String readUTF8() {
    byte[] bytes = new byte[buffer.readUnsignedShort()];
    buffer.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public void close() {

  }

}
