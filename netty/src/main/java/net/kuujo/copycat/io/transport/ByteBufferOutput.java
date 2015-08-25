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
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.Bytes;

import java.nio.charset.StandardCharsets;

/**
 * Byte buffer output.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ByteBufferOutput implements BufferOutput<ByteBufferOutput> {
  ByteBuf buffer;

  /**
   * Sets the underlying byte buffer.
   */
  ByteBufferOutput setByteBuf(ByteBuf buffer) {
    this.buffer = buffer;
    return this;
  }

  /**
   * Ensures that {@code size} bytes can be written to the buffer.
   */
  private void checkWrite(int size) {
    // If the buffer does not have enough bytes remaining, attempt to discard some of the read bytes.
    // It is possible that the buffer could discard 0 bytes, so we ensure the buffer is writable after
    // discarding some read bytes, and if not discard all read bytes.
    if (!buffer.isWritable(size)) {
      buffer.discardSomeReadBytes();
      if (!buffer.isWritable(size)) {
        buffer.discardReadBytes();
      }
    }
  }

  @Override
  public ByteBufferOutput write(Buffer buffer) {
    int size = Math.min((int) buffer.remaining(), this.buffer.writableBytes());
    checkWrite(size);
    byte[] bytes = new byte[size];
    buffer.read(bytes);
    this.buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public ByteBufferOutput write(Bytes bytes) {
    int size = Math.min((int) bytes.size(), buffer.writableBytes());
    checkWrite(size);
    byte[] b = new byte[size];
    bytes.read(0, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public ByteBufferOutput write(byte[] bytes) {
    checkWrite(bytes.length);
    buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public ByteBufferOutput write(Bytes bytes, long offset, long length) {
    int size = Math.min((int) bytes.size(), (int) length);
    checkWrite(size);
    byte[] b = new byte[size];
    bytes.read(offset, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public ByteBufferOutput write(byte[] bytes, long offset, long length) {
    checkWrite((int) length);
    buffer.writeBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public ByteBufferOutput writeByte(int b) {
    checkWrite(Bytes.BYTE);
    buffer.writeByte(b);
    return this;
  }

  @Override
  public ByteBufferOutput writeUnsignedByte(int b) {
    checkWrite(Bytes.BYTE);
    buffer.writeByte(b);
    return this;
  }

  @Override
  public ByteBufferOutput writeChar(char c) {
    checkWrite(Bytes.CHARACTER);
    buffer.writeChar(c);
    return this;
  }

  @Override
  public ByteBufferOutput writeShort(short s) {
    checkWrite(Bytes.SHORT);
    buffer.writeShort(s);
    return this;
  }

  @Override
  public ByteBufferOutput writeUnsignedShort(int s) {
    checkWrite(Bytes.SHORT);
    buffer.writeShort(s);
    return this;
  }

  @Override
  public ByteBufferOutput writeInt(int i) {
    checkWrite(Bytes.INTEGER);
    buffer.writeInt(i);
    return this;
  }

  @Override
  public ByteBufferOutput writeUnsignedInt(long i) {
    checkWrite(Bytes.INTEGER);
    buffer.writeInt((int) i);
    return this;
  }

  @Override
  public ByteBufferOutput writeMedium(int m) {
    checkWrite(Bytes.MEDIUM);
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public ByteBufferOutput writeUnsignedMedium(int m) {
    checkWrite(Bytes.MEDIUM);
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public ByteBufferOutput writeLong(long l) {
    checkWrite(Bytes.LONG);
    buffer.writeLong(l);
    return this;
  }

  @Override
  public ByteBufferOutput writeFloat(float f) {
    checkWrite(Bytes.FLOAT);
    buffer.writeFloat(f);
    return this;
  }

  @Override
  public ByteBufferOutput writeDouble(double d) {
    checkWrite(Bytes.DOUBLE);
    buffer.writeDouble(d);
    return this;
  }

  @Override
  public ByteBufferOutput writeBoolean(boolean b) {
    checkWrite(Bytes.BOOLEAN);
    buffer.writeBoolean(b);
    return this;
  }

  @Override
  public ByteBufferOutput writeString(String s) {
    return writeUTF8(s);
  }

  @Override
  public ByteBufferOutput writeUTF8(String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    checkWrite(Bytes.SHORT + bytes.length);
    buffer.writeShort(bytes.length).writeBytes(bytes);
    return this;
  }

  @Override
  public ByteBufferOutput flush() {
    return this;
  }

  @Override
  public void close() {

  }

}
