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

import net.kuujo.copycat.io.util.IllegalReferenceException;
import net.kuujo.copycat.io.util.ReferenceCounted;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer reader.
 * <p>
 * Buffer readers expose a simple position based interface for reading buffer bytes. When a new buffer reader is created,
 * the reader will start at the requested position in the buffer and may only advance forward while reading bytes from
 * the underlying {@link Bytes} instance. Readers are intended for single use only. Once all the necessary bytes have
 * been read by the reader, close the reader via {@link BufferReader#close()} to dereference the reader. Block readers
 * are lightweight, and the underlying buffer will recycle the reader once it's closed to make it accessible to future
 * {@link Buffer#reader()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferReader implements BufferInput<BufferReader>, ReferenceCounted<BufferReader>, AutoCloseable {
  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private final Bytes bytes;
  private final ReferenceManager<BufferReader> referenceManager;
  private long offset;
  private long position;
  private long capacity;
  private boolean open;

  public BufferReader(Bytes bytes, long offset, long capacity, ReferenceManager<BufferReader> referenceManager) {
    if (bytes == null)
      throw new NullPointerException("bytes cannot be null");
    if (referenceManager == null)
      throw new NullPointerException("reference manager cannot be null");
    this.bytes = bytes;
    this.referenceManager = referenceManager;
    reset(offset, capacity);
  }

  /**
   * Resets the reader.
   */
  BufferReader reset(long offset, long capacity) {
    if (offset < 0)
      throw new IllegalArgumentException("offset cannot be negative");
    if (offset > bytes.size())
      throw new IllegalArgumentException("offset cannot be greater than the underlying bytes size");
    if (capacity < 0)
      throw new IllegalArgumentException("capacity cannot be negative");
    if (offset + capacity > bytes.size())
      throw new IllegalArgumentException("capacity cannot be greater than the underlying bytes size");
    this.position = 0;
    this.offset = offset;
    this.capacity = capacity;
    referenceCount.set(1);
    open = true;
    return this;
  }

  /**
   * Returns the reader's bytes.
   */
  protected Bytes bytes() {
    return bytes;
  }

  /**
   * Returns the reader's offset.
   */
  protected long offset() {
    return offset;
  }

  /**
   * Sets the reader position.
   */
  protected BufferReader position(long position) {
    this.position = position;
    return this;
  }

  /**
   * Returns the current position of the reader.
   * <p>
   * The position is incremented each time the reader reads a byte from the underlying {@link Buffer}.
   *
   * @return The current position of the reader.
   */
  public long position() {
    return position;
  }

  /**
   * Returns the reader capacity.
   * <p>
   * The capacity is defined when the reader is created and is fixed throughout the lifetime of the reader.
   *
   * @return The reader capacity.
   *
   * @see Buffer#reader(long, long)
   */
  public long capacity() {
    return capacity;
  }

  /**
   * Returns the number of bytes remaining in the reader.
   *
   * @return The number of bytes remaining in the reader.
   */
  public long remaining() {
    return capacity - position;
  }

  /**
   * Returns whether there are bytes remaining in the reader.
   *
   * @return Indicates whether there are bytes left to be read in the reader.
   */
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  /**
   * Resets the reader position to {@code 0}.
   *
   * @return The buffer reader.
   */
  public BufferReader reset() {
    position = 0;
    return this;
  }

  @Override
  public BufferReader acquire() {
    referenceCount.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (referenceCount.decrementAndGet() == 0) {
      referenceManager.release(this);
    }
  }

  @Override
  public int references() {
    return referenceCount.get();
  }

  /**
   * Returns the absolute offset for the given relative offset.
   */
  private long offset(long offset) {
    return this.offset + offset;
  }

  /**
   * Checks that the reader is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("writer not open");
    if (referenceCount.get() == 0)
      throw new IllegalReferenceException("buffer reader has no active references");
  }

  /**
   * Checks bounds for a read for the given length.
   */
  protected long checkRead(long length) {
    checkOpen();
    if (position + length > capacity)
      throw new BufferUnderflowException();
    long previousPosition = this.position;
    this.position = previousPosition + length;
    return offset(previousPosition);
  }

  @Override
  public BufferReader read(Bytes bytes) {
    bytes.read(checkRead(bytes.size()), bytes, 0, bytes.size());
    return this;
  }

  @Override
  public BufferReader read(Bytes bytes, long offset, long length) {
    this.bytes.read(checkRead(length), bytes, offset, length);
    return this;
  }

  @Override
  public BufferReader read(byte[] bytes) {
    this.bytes.read(checkRead(bytes.length), bytes, 0, bytes.length);
    return this;
  }

  @Override
  public BufferReader read(byte[] bytes, long offset, long length) {
    this.bytes.read(checkRead(length), bytes, offset, length);
    return this;
  }

  @Override
  public BufferReader read(Buffer buffer) {
    long length = Math.min(buffer.remaining(), remaining());
    read(buffer.bytes(), buffer.offset() + buffer.position(), length);
    buffer.position(buffer.position() + length);
    return this;
  }

  @Override
  public BufferReader read(BufferWriter writer) {
    long length = Math.min(writer.remaining(), remaining());
    read(writer.bytes(), writer.offset() + writer.position(), length);
    writer.position(writer.position() + length);
    return this;
  }

  @Override
  public int readByte() {
    return bytes.readByte(checkRead(Byte.BYTES));
  }

  @Override
  public int readUnsignedByte() {
    return bytes.readUnsignedByte(checkRead(Byte.BYTES));
  }

  @Override
  public char readChar() {
    return bytes.readChar(checkRead(Character.BYTES));
  }

  @Override
  public short readShort() {
    return bytes.readShort(checkRead(Short.BYTES));
  }

  @Override
  public int readUnsignedShort() {
    return bytes.readUnsignedShort(checkRead(Short.BYTES));
  }

  @Override
  public int readMedium() {
    return bytes.readMedium(checkRead(3));
  }

  @Override
  public int readUnsignedMedium() {
    return bytes.readUnsignedMedium(checkRead(3));
  }

  @Override
  public int readInt() {
    return bytes.readInt(checkRead(Integer.BYTES));
  }

  @Override
  public long readUnsignedInt() {
    return bytes.readUnsignedInt(checkRead(Integer.BYTES));
  }

  @Override
  public long readLong() {
    return bytes.readLong(checkRead(Long.BYTES));
  }

  @Override
  public float readFloat() {
    return bytes.readFloat(checkRead(Float.BYTES));
  }

  @Override
  public double readDouble() {
    return bytes.readDouble(checkRead(Double.BYTES));
  }

  @Override
  public boolean readBoolean() {
    return bytes.readBoolean(checkRead(Byte.BYTES));
  }

  @Override
  public String readUTF8() {
    byte[] bytes = new byte[readUnsignedShort()];
    read(bytes, 0, bytes.length);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    referenceManager.release(this);
    open = false;
  }

}
