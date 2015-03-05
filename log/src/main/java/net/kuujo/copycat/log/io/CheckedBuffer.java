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
package net.kuujo.copycat.log.io;

/**
 * Abstract buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CheckedBuffer implements Buffer {
  private final Bytes bytes;
  private final BufferNavigator navigator;

  protected CheckedBuffer(Bytes bytes) {
    this(bytes, 0);
  }

  protected CheckedBuffer(Bytes bytes, long offset) {
    if (bytes == null)
      throw new NullPointerException("bytes cannot be null");
    this.bytes = bytes;
    this.navigator = new BufferNavigator(offset, bytes.size());
  }

  @Override
  public Bytes bytes() {
    return bytes;
  }

  @Override
  public Buffer slice() {
    return new CheckedBuffer(bytes, navigator.position());
  }

  @Override
  public long capacity() {
    return navigator.capacity();
  }

  @Override
  public long position() {
    return navigator.position();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer position(long position) {
    navigator.position(position);
    return this;
  }

  @Override
  public long limit() {
    return navigator.limit();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer limit(long limit) {
    navigator.limit(limit);
    return this;
  }

  @Override
  public long remaining() {
    return navigator.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return navigator.hasRemaining();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer flip() {
    navigator.flip();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer mark() {
    navigator.mark();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer rewind() {
    navigator.rewind();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer reset() {
    navigator.reset();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Buffer clear() {
    navigator.clear();
    return this;
  }

  @Override
  public Buffer read(Buffer output) {
    output.write(this);
    return this;
  }

  @Override
  public Buffer read(Bytes bytes) {
    bytes.write(bytes, navigator.getAndSetPosition(navigator.checkRead(position(), remaining())), remaining());
    return this;
  }

  @Override
  public Buffer read(Bytes bytes, long offset, long length) {
    bytes.write(bytes, offset, length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes) {
    this.bytes.read(bytes, navigator.getAndSetPosition(navigator.checkRead(position(), bytes.length)), bytes.length);
    return this;
  }

  @Override
  public Buffer read(byte[] bytes, long offset, long length) {
    navigator.checkRead(offset, length);
    this.bytes.read(bytes, offset, length);
    return this;
  }

  @Override
  public int readByte() {
    return bytes.readByte(navigator.getAndSetPosition(navigator.checkRead(position(), Byte.BYTES)));
  }

  @Override
  public int readByte(long offset) {
    navigator.checkRead(offset, Byte.BYTES);
    return bytes.readByte(offset);
  }

  @Override
  public char readChar() {
    return bytes.readChar(navigator.getAndSetPosition(navigator.checkRead(position(), Character.BYTES)));
  }

  @Override
  public char readChar(long offset) {
    navigator.checkRead(offset, Character.BYTES);
    return bytes.readChar(offset);
  }

  @Override
  public short readShort() {
    navigator.checkRead(position(), Short.BYTES);
    return bytes.readShort(navigator.getAndSetPosition(navigator.checkRead(position(), Short.BYTES)));
  }

  @Override
  public short readShort(long offset) {
    navigator.checkRead(offset, Short.BYTES);
    return bytes.readShort(offset);
  }

  @Override
  public int readInt() {
    navigator.checkRead(position(), Integer.BYTES);
    return bytes.readInt(navigator.getAndSetPosition(navigator.checkRead(position(), Integer.BYTES)));
  }

  @Override
  public int readInt(long offset) {
    navigator.checkRead(offset, Integer.BYTES);
    return bytes.readInt(offset);
  }

  @Override
  public long readLong() {
    navigator.checkRead(position(), Long.BYTES);
    return bytes.readLong(navigator.getAndSetPosition(navigator.checkRead(position(), Long.BYTES)));
  }

  @Override
  public long readLong(long offset) {
    navigator.checkRead(offset, Long.BYTES);
    return bytes.readLong(offset);
  }

  @Override
  public float readFloat() {
    navigator.checkRead(position(), Float.BYTES);
    return bytes.readFloat(navigator.getAndSetPosition(navigator.checkRead(position(), Float.BYTES)));
  }

  @Override
  public float readFloat(long offset) {
    navigator.checkRead(offset, Float.BYTES);
    return bytes.readFloat(offset);
  }

  @Override
  public double readDouble() {
    navigator.checkRead(position(), Double.BYTES);
    return bytes.readDouble(navigator.getAndSetPosition(navigator.checkRead(position(), Double.BYTES)));
  }

  @Override
  public double readDouble(long offset) {
    navigator.checkRead(offset, Double.BYTES);
    return bytes.readDouble(offset);
  }

  @Override
  public boolean readBoolean() {
    navigator.checkRead(position(), Byte.BYTES);
    return bytes.readBoolean(navigator.getAndSetPosition(navigator.checkRead(position(), Byte.BYTES)));
  }

  @Override
  public boolean readBoolean(long offset) {
    navigator.checkRead(position(), Byte.BYTES);
    return bytes.readBoolean(offset);
  }

  @Override
  public Buffer write(Buffer buffer) {
    long position = position();
    long remaining = buffer.remaining();
    navigator.checkWrite(position, remaining);
    for (long i = 0; i < remaining; i++) {
      bytes.writeByte(navigator.getAndSetPosition(++position), buffer.readByte());
    }
    return this;
  }

  @Override
  public Buffer write(Bytes bytes) {
    long startPosition = position();
    navigator.checkWrite(startPosition, bytes.size());
    for (long i = 0; i < bytes.size(); i++) {
      this.bytes.writeByte(navigator.getAndSetPosition(startPosition + i), bytes.readByte(i));
    }
    return this;
  }

  @Override
  public Buffer write(Bytes bytes, long offset, long length) {
    navigator.checkWrite(offset, length);
    this.bytes.write(bytes, offset, length);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes) {
    this.bytes.write(bytes, navigator.getAndSetPosition(navigator.checkWrite(position(), bytes.length)), bytes.length);
    return this;
  }

  @Override
  public Buffer write(byte[] bytes, long offset, long length) {
    navigator.checkWrite(offset, length);
    this.bytes.write(bytes, offset, length);
    return this;
  }

  @Override
  public Buffer writeByte(int b) {
    bytes.writeByte(navigator.getAndSetPosition(navigator.checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public Buffer writeByte(long offset, int b) {
    navigator.checkWrite(offset, Byte.BYTES);
    bytes.writeByte(offset, b);
    return this;
  }

  @Override
  public Buffer writeChar(char c) {
    bytes.writeChar(navigator.getAndSetPosition(navigator.checkWrite(position(), Character.BYTES)), c);
    return this;
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    navigator.checkWrite(offset, Character.BYTES);
    bytes.writeChar(offset, c);
    return this;
  }

  @Override
  public Buffer writeShort(short s) {
    bytes.writeShort(navigator.getAndSetPosition(navigator.checkWrite(position(), Short.BYTES)), s);
    return this;
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    navigator.checkWrite(offset, Short.BYTES);
    bytes.writeShort(offset, s);
    return this;
  }

  @Override
  public Buffer writeInt(int i) {
    bytes.writeInt(navigator.getAndSetPosition(navigator.checkWrite(position(), Integer.BYTES)), i);
    return this;
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    navigator.checkWrite(offset, Integer.BYTES);
    bytes.writeInt(offset, i);
    return this;
  }

  @Override
  public Buffer writeLong(long l) {
    bytes.writeLong(navigator.getAndSetPosition(navigator.checkWrite(position(), Long.BYTES)), l);
    return this;
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    navigator.checkWrite(offset, Long.BYTES);
    bytes.writeLong(offset, l);
    return this;
  }

  @Override
  public Buffer writeFloat(float f) {
    bytes.writeFloat(navigator.getAndSetPosition(navigator.checkWrite(position(), Float.BYTES)), f);
    return this;
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    navigator.checkWrite(offset, Float.BYTES);
    bytes.writeFloat(offset, f);
    return this;
  }

  @Override
  public Buffer writeDouble(double d) {
    bytes.writeDouble(navigator.getAndSetPosition(navigator.checkWrite(position(), Double.BYTES)), d);
    return this;
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    navigator.checkWrite(offset, Double.BYTES);
    bytes.writeDouble(offset, d);
    return this;
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    bytes.writeBoolean(navigator.getAndSetPosition(navigator.checkWrite(position(), Byte.BYTES)), b);
    return this;
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    navigator.checkWrite(offset, Byte.BYTES);
    bytes.writeBoolean(offset, b);
    return this;
  }

  @Override
  public void close() throws Exception {

  }

}
