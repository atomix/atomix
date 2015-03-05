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
 * Buffer reader.
 * <p>
 * Buffer readers expose a simple position based interface for reading buffer bytes. When a new buffer reader is created,
 * the reader will start at the requested position in the buffer and may only advance forward while reading bytes from
 * the underlying {@link net.kuujo.copycat.log.io.Bytes} instance. Readers are intended for single use only. Once all the necessary bytes have
 * been read by the reader, close the reader via {@link net.kuujo.copycat.log.io.BufferReader#close()} to dereference the reader. Block readers
 * are lightweight, and the underlying buffer will recycle the reader once it's closed to make it accessible to future
 * {@link net.kuujo.copycat.log.io.Block#reader()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferReader implements BufferInput<BufferReader>, ReferenceCounted<BufferReader>, AutoCloseable {
  private final AtomicInteger references;
  private final Buffer buffer;
  private final ReferenceManager<BufferReader> referenceManager;
  private final BufferNavigator bufferNavigator;
  private boolean open;

  public BufferReader(Buffer buffer, long offset, long limit, ReferenceManager<BufferReader> referenceManager) {
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
    this.references = new AtomicInteger(1);

    // Initialize the navigator to the correct position and limit in order to make sure
    // exception messages are accurate (don't just use the offset and capacity).
    this.bufferNavigator = new BufferNavigator(0, this.buffer.limit());
    bufferNavigator.position(offset);
    bufferNavigator.limit(limit);
  }

  /**
   * Resets the reader.
   */
  BufferReader reset(long offset, long limit) {
    bufferNavigator.clear();
    bufferNavigator.position(offset);
    bufferNavigator.limit(limit);
    references.set(1);
    open = true;
    return this;
  }

  /**
   * Returns the current position of the reader.
   *
   * @return The current position of the reader.
   */
  public long position() {
    return bufferNavigator.position();
  }

  /**
   * Returns the reader limit.
   *
   * @return The reader limit.
   */
  public long limit() {
    return bufferNavigator.limit();
  }

  @Override
  public BufferReader acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0) {
      referenceManager.release(this);
    }
  }

  @Override
  public int references() {
    return references.get();
  }

  /**
   * Checks that the reader is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("writer not open");
    if (references.get() == 0)
      throw new IllegalReferenceException("buffer reader has no active references");
  }

  @Override
  public BufferReader read(Bytes bytes) {
    checkOpen();
    buffer.bytes().read(bytes, bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(bytes.size())), bytes.size());
    return this;
  }

  @Override
  public BufferReader read(byte[] bytes) {
    checkOpen();
    buffer.bytes().read(bytes, bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(bytes.length)), bytes.length);
    return this;
  }

  @Override
  public int readByte() {
    checkOpen();
    return buffer.bytes().readByte(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Byte.BYTES)));
  }

  @Override
  public char readChar() {
    checkOpen();
    return buffer.bytes().readChar(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Character.BYTES)));
  }

  @Override
  public short readShort() {
    checkOpen();
    return buffer.bytes().readShort(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Short.BYTES)));
  }

  @Override
  public int readInt() {
    checkOpen();
    return buffer.bytes().readInt(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Integer.BYTES)));
  }

  @Override
  public long readLong() {
    checkOpen();
    return buffer.bytes().readLong(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Long.BYTES)));
  }

  @Override
  public float readFloat() {
    checkOpen();
    return buffer.bytes().readFloat(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Float.BYTES)));
  }

  @Override
  public double readDouble() {
    checkOpen();
    return buffer.bytes().readDouble(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Double.BYTES)));
  }

  @Override
  public boolean readBoolean() {
    checkOpen();
    return buffer.bytes().readBoolean(bufferNavigator.getAndSetPosition(bufferNavigator.checkRead(Byte.BYTES)));
  }

  @Override
  public void close() {
    referenceManager.release(this);
    open = false;
  }

}
