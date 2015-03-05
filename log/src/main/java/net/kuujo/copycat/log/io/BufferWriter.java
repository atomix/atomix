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

import net.kuujo.copycat.log.io.util.IllegalReferenceException;
import net.kuujo.copycat.log.io.util.ReferenceCounted;
import net.kuujo.copycat.log.io.util.ReferenceManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer writer.
 * <p>
 * Block writers expose a simple position based interface for reading block bytes. When a new block writer is created,
 * the writer will start at the requested position in the block and may only advance forward while reading bytes from
 * the underlying {@link net.kuujo.copycat.log.io.Bytes} instance. writers are intended for single use only. Once all the necessary bytes have
 * been read by the writer, close the writer via {@link net.kuujo.copycat.log.io.BufferWriter#close()} to dereference the writer. Block writers
 * are lightweight, and the underlying block will recycle the writer once it's closed to make it accessible to future
 * {@link net.kuujo.copycat.log.io.Block#writer()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferWriter<T extends BufferWriter<T, U>, U extends Buffer> implements BufferOutput<T>, ReferenceCounted<T>, AutoCloseable {
  private final AtomicInteger referenceCount;
  private final Buffer buffer;
  private final ReferenceManager<T> referenceManager;
  private final BufferNavigator bufferNavigator;
  private boolean open;

  public BufferWriter(Buffer buffer, long offset, long limit, ReferenceManager<T> referenceManager) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    if (offset < 0)
      throw new IllegalArgumentException("offset cannot be negative");
    if (offset > buffer.limit())
      throw new IllegalArgumentException("offset cannot be greater than the underlying buffer's limit");
    if (limit < 0)
      throw new IllegalArgumentException("limit cannot be negative");
    if (limit > buffer.limit())
      throw new IllegalArgumentException("limit cannot be greater than the underlying buffer's limit");
    if (referenceManager == null)
      throw new NullPointerException("reference manager cannot be null");
    this.buffer = buffer;
    this.referenceManager = referenceManager;
    referenceCount = new AtomicInteger(1);

    // Initialize the navigator to the correct position and limit in order to make sure
    // exception messages are accurate (don't just use the offset and capacity).
    this.bufferNavigator = new BufferNavigator(0, this.buffer.limit());
    bufferNavigator.position(offset);
    bufferNavigator.limit(limit);
  }

  /**
   * Resets the writer.
   */
  BufferWriter reset(long offset, long limit) {
    bufferNavigator.clear();
    bufferNavigator.position(offset);
    bufferNavigator.limit(limit);
    referenceCount.set(1);
    open = true;
    return this;
  }

  /**
   * Returns the current position of the writer.
   *
   * @return The current position of the writer.
   */
  public long position() {
    return bufferNavigator.position();
  }

  /**
   * Returns the writer limit.
   *
   * @return The writer limit.
   */
  public long limit() {
    return bufferNavigator.limit();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T acquire() {
    referenceCount.incrementAndGet();
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void release() {
    if (referenceCount.decrementAndGet() == 0) {
      referenceManager.release((T) this);
    }
  }

  @Override
  public int references() {
    return referenceCount.get();
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

  @Override
  @SuppressWarnings("unchecked")
  public T write(Bytes bytes) {
    checkOpen();
    buffer.bytes().write(bytes, bufferNavigator.getAndSetPosition(bufferNavigator.checkWrite(bytes.size())), bytes.size());
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T write(byte[] bytes) {
    checkOpen();
    buffer.bytes().write(bytes, bufferNavigator.getAndSetPosition(bufferNavigator.checkWrite(bytes.length)), bytes.length);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeByte(int b) {
    checkOpen();
    buffer.bytes().writeByte(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Byte.BYTES)), b);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeChar(char c) {
    checkOpen();
    buffer.bytes().writeChar(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Character.BYTES)), c);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeShort(short s) {
    checkOpen();
    buffer.bytes().writeShort(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Short.BYTES)), s);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeInt(int i) {
    checkOpen();
    buffer.bytes().writeInt(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Integer.BYTES)), i);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeLong(long l) {
    checkOpen();
    buffer.bytes().writeLong(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Long.BYTES)), l);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeFloat(float f) {
    checkOpen();
    buffer.bytes().writeFloat(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Float.BYTES)), f);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeDouble(double d) {
    checkOpen();
    buffer.bytes().writeDouble(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Double.BYTES)), d);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T writeBoolean(boolean b) {
    checkOpen();
    buffer.bytes().writeBoolean(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Byte.BYTES)), b);
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    referenceManager.release((T) this);
    open = false;
  }

}
