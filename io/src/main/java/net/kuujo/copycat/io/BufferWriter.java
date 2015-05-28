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

import java.nio.BufferOverflowException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer writer.
 * <p>
 * Block writers expose a simple position based interface for reading block bytes. When a new block writer is created,
 * the writer will start at the requested position in the block and may only advance forward while reading bytes from
 * the underlying {@link Bytes} instance. writers are intended for single use only. Once all the necessary bytes have
 * been read by the writer, close the writer via {@link BufferWriter#close()} to dereference the writer. Block writers
 * are lightweight, and the underlying block will recycle the writer once it's closed to make it accessible to future
 * {@link Buffer#writer()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferWriter implements BufferOutput<BufferWriter>, ReferenceCounted<BufferWriter>, AutoCloseable {
  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private final Bytes bytes;
  private final ReferenceManager<BufferWriter> referenceManager;
  private long offset;
  private long position;
  private long capacity;
  private boolean open;

  public BufferWriter(Bytes bytes, long offset, long capacity, ReferenceManager<BufferWriter> referenceManager) {
    if (bytes == null)
      throw new NullPointerException("bytes cannot be null");
    if (referenceManager == null)
      throw new NullPointerException("reference manager cannot be null");
    this.bytes = bytes;
    this.referenceManager = referenceManager;
    reset(offset, capacity);
  }

  /**
   * Resets the writer.
   */
  BufferWriter reset(long offset, long capacity) {
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
   * Returns the writer's bytes.
   */
  protected Bytes bytes() {
    return bytes;
  }

  /**
   * Returns the writer's offset.
   */
  protected long offset() {
    return offset;
  }

  /**
   * Sets the writer position.
   */
  protected BufferWriter position(long position) {
    this.position = position;
    return this;
  }

  /**
   * Returns the current position of the writer.
   * <p>
   * The position is incremented each time the writer writes a byte to the underlying {@link Buffer}.
   *
   * @return The current position of the writer.
   */
  public long position() {
    return position;
  }

  /**
   * Returns the writer capacity.
   * <p>
   * The capacity is defined when the writer is created and is fixed throughout the lifetime of the writer.
   *
   * @return The writer capacity.
   *
   * @see Buffer#writer(long, long)
   */
  public long capacity() {
    return capacity;
  }

  /**
   * Returns the number of bytes remaining in the writer.
   *
   * @return The number of bytes remaining in the writer.
   */
  public long remaining() {
    return capacity - position;
  }

  /**
   * Returns whether there are bytes remaining in the writer.
   *
   * @return Indicates whether there are bytes left to be read in the writer.
   */
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  /**
   * Resets the writer position to {@code 0}.
   *
   * @return The buffer writer.
   */
  public BufferWriter reset() {
    position = 0;
    return this;
  }

  @Override
  public BufferWriter acquire() {
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
   * Checks that the writer is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("writer not open");
    if (referenceCount.get() == 0)
      throw new IllegalReferenceException("buffer writer has no active references");
  }

  /**
   * Checks bounds for a write for the given length.
   */
  protected long checkWrite(long length) {
    checkOpen();
    if (position + length > capacity)
      throw new BufferOverflowException();
    long previousPosition = this.position;
    this.position = previousPosition + length;
    return offset(previousPosition);
  }

  @Override
  public BufferWriter write(Bytes bytes) {
    bytes.write(checkWrite(bytes.size()), bytes, 0, bytes.size());
    return this;
  }

  @Override
  public BufferWriter write(Bytes bytes, long offset, long length) {
    bytes.write(checkWrite(length), bytes, offset, length);
    return this;
  }

  @Override
  public BufferWriter write(byte[] bytes) {
    this.bytes.write(checkWrite(bytes.length), bytes, 0, bytes.length);
    return this;
  }

  @Override
  public BufferWriter write(byte[] bytes, long offset, long length) {
    this.bytes.write(checkWrite(length), bytes, offset, length);
    return this;
  }

  @Override
  public BufferWriter write(Buffer buffer) {
    long length = Math.min(buffer.remaining(), remaining());
    write(buffer.bytes(), buffer.offset() + buffer.position(), length);
    buffer.position(buffer.position() + length);
    return this;
  }

  @Override
  public BufferWriter write(BufferReader reader) {
    long length = Math.min(reader.remaining(), remaining());
    write(reader.bytes(), reader.offset() + reader.position(), length);
    reader.position(reader.position() + length);
    return this;
  }

  @Override
  public BufferWriter writeByte(int b) {
    bytes.writeByte(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public BufferWriter writeUnsignedByte(int b) {
    bytes.writeUnsignedByte(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public BufferWriter writeChar(char c) {
    bytes.writeChar(checkWrite(Character.BYTES), c);
    return this;
  }

  @Override
  public BufferWriter writeShort(short s) {
    bytes.writeShort(checkWrite(Short.BYTES), s);
    return this;
  }

  @Override
  public BufferWriter writeUnsignedShort(int s) {
    bytes.writeUnsignedShort(checkWrite(Short.BYTES), s);
    return this;
  }

  @Override
  public BufferWriter writeMedium(int m) {
    bytes.writeMedium(checkWrite(3), m);
    return this;
  }

  @Override
  public BufferWriter writeUnsignedMedium(int m) {
    bytes.writeUnsignedMedium(checkWrite(3), m);
    return this;
  }

  @Override
  public BufferWriter writeInt(int i) {
    bytes.writeInt(checkWrite(Integer.BYTES), i);
    return this;
  }

  @Override
  public BufferWriter writeUnsignedInt(long i) {
    bytes.writeUnsignedInt(checkWrite(Integer.BYTES), i);
    return this;
  }

  @Override
  public BufferWriter writeLong(long l) {
    bytes.writeLong(checkWrite(Long.BYTES), l);
    return this;
  }

  @Override
  public BufferWriter writeFloat(float f) {
    bytes.writeFloat(checkWrite(Float.BYTES), f);
    return this;
  }

  @Override
  public BufferWriter writeDouble(double d) {
    bytes.writeDouble(checkWrite(Double.BYTES), d);
    return this;
  }

  @Override
  public BufferWriter writeBoolean(boolean b) {
    bytes.writeBoolean(checkWrite(Byte.BYTES), b);
    return this;
  }

  @Override
  public BufferWriter writeUTF8(String s) {
    if (s == null) {
      return writeByte(0);
    } else {
      writeByte(1);
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      return writeUnsignedShort(bytes.length)
        .write(bytes, 0, bytes.length);
    }
  }

  @Override
  public BufferWriter flush() {
    checkOpen();
    bytes.flush();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    referenceManager.release(this);
    open = false;
  }

}
