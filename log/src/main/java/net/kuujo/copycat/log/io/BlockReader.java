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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Block reader.
 * <p>
 * Block readers expose a simple position based interface for reading block bytes. When a new block reader is created,
 * the reader will start at the requested position in the block and may only advance forward while reading bytes from
 * the underlying {@link Bytes} instance. Readers are intended for single use only. Once all the necessary bytes have
 * been read by the reader, close the reader via {@link BlockReader#close()} to dereference the reader. Block readers
 * are lightweight, and the underlying block will recycle the reader once it's closed to make it accessible to future
 * {@link Block#reader()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BlockReader implements BufferInput<BlockReader>, ReferenceCounted<BlockReader>, AutoCloseable {
  private final AtomicInteger references;
  private final Block block;
  private final BufferNavigator navigator;
  private boolean open;

  public BlockReader(Block block, long offset, long limit) {
    if (block == null)
      throw new NullPointerException("block cannot be null");
    if (offset < 0)
      throw new IllegalArgumentException("offset cannot be negative");
    if (offset > block.limit())
      throw new IllegalArgumentException("offset cannot be greater than the underlying block's limit");
    if (limit < 0)
      throw new IllegalArgumentException("limit cannot be negative");
    if (limit > block.limit())
      throw new IllegalArgumentException("limit cannot be greater than the underlying block's limit");
    this.block = block;
    this.references = new AtomicInteger(1);
    block.acquire();

    // Initialize the navigator to the correct position and limit in order to make sure
    // exception messages are accurate (don't just use the offset and capacity).
    this.navigator = new BufferNavigator(0, block.limit());
    navigator.position(offset);
    navigator.limit(limit);
  }

  /**
   * Resets the reader.
   */
  BlockReader reset(long offset, long limit) {
    navigator.clear();
    navigator.position(offset);
    navigator.limit(limit);
    block.acquire();
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
    return navigator.position();
  }

  /**
   * Returns the reader limit.
   *
   * @return The reader limit.
   */
  public long limit() {
    return navigator.limit();
  }

  @Override
  public BlockReader acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0) {
      block.readers().release(this);
      block.release();
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
      throw new IllegalReferenceException("block reader has no active references");
  }

  @Override
  public BlockReader read(Bytes bytes) {
    checkOpen();
    block.bytes().read(bytes, navigator.getAndSetPosition(navigator.checkRead(bytes.size())), bytes.size());
    return this;
  }

  @Override
  public BlockReader read(byte[] bytes) {
    checkOpen();
    block.bytes().read(bytes, navigator.getAndSetPosition(navigator.checkRead(bytes.length)), bytes.length);
    return this;
  }

  @Override
  public int readByte() {
    checkOpen();
    return block.bytes().readByte(navigator.getAndSetPosition(navigator.checkRead(Byte.BYTES)));
  }

  @Override
  public char readChar() {
    checkOpen();
    return block.bytes().readChar(navigator.getAndSetPosition(navigator.checkRead(Character.BYTES)));
  }

  @Override
  public short readShort() {
    checkOpen();
    return block.bytes().readShort(navigator.getAndSetPosition(navigator.checkRead(Short.BYTES)));
  }

  @Override
  public int readInt() {
    checkOpen();
    return block.bytes().readInt(navigator.getAndSetPosition(navigator.checkRead(Integer.BYTES)));
  }

  @Override
  public long readLong() {
    checkOpen();
    return block.bytes().readLong(navigator.getAndSetPosition(navigator.checkRead(Long.BYTES)));
  }

  @Override
  public float readFloat() {
    checkOpen();
    return block.bytes().readFloat(navigator.getAndSetPosition(navigator.checkRead(Float.BYTES)));
  }

  @Override
  public double readDouble() {
    checkOpen();
    return block.bytes().readDouble(navigator.getAndSetPosition(navigator.checkRead(Double.BYTES)));
  }

  @Override
  public boolean readBoolean() {
    checkOpen();
    return block.bytes().readBoolean(navigator.getAndSetPosition(navigator.checkRead(Byte.BYTES)));
  }

  @Override
  public void close() {
    block.readers().release(this);
    block.release();
    open = false;
  }

}
