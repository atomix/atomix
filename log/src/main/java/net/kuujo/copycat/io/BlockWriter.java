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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer writer.
 * <p>
 * Block writers expose a simple position based interface for reading block bytes. When a new block writer is created,
 * the writer will start at the requested position in the block and may only advance forward while reading bytes from
 * the underlying {@link Bytes} instance. writers are intended for single use only. Once all the necessary bytes have
 * been read by the writer, close the writer via {@link BlockWriter#close()} to dereference the writer. Block writers
 * are lightweight, and the underlying block will recycle the writer once it's closed to make it accessible to future
 * {@link Block#writer()} calls.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BlockWriter implements BufferOutput<BlockWriter>, ReferenceCounted<BlockWriter>, AutoCloseable {
  private final AtomicInteger references;
  private final Block block;
  private final BufferNavigator navigator;
  private boolean open;

  public BlockWriter(Block block, long offset, long limit) {
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
    references = new AtomicInteger(1);
    block.acquire();

    // Initialize the navigator to the correct position and limit in order to make sure
    // exception messages are accurate (don't just use the offset and capacity).
    this.navigator = new BufferNavigator(0, block.limit());
    navigator.position(offset);
    navigator.limit(limit);
  }

  /**
   * Resets the writer.
   */
  BlockWriter reset(long offset, long limit) {
    navigator.clear();
    navigator.position(offset);
    navigator.limit(limit);
    block.acquire();
    references.set(1);
    open = true;
    return this;
  }

  /**
   * Returns the current position of the writer.
   *
   * @return The current position of the writer.
   */
  public long position() {
    return navigator.position();
  }

  /**
   * Returns the writer limit.
   *
   * @return The writer limit.
   */
  public long limit() {
    return navigator.limit();
  }

  @Override
  public BlockWriter acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0) {
      block.writers().release(this);
      block.release();
    }
  }

  @Override
  public int references() {
    return references.get();
  }

  /**
   * Checks that the writer is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("writer not open");
    if (references.get() == 0)
      throw new IllegalReferenceException("block writer has no active references");
  }

  @Override
  public BlockWriter write(byte[] bytes) {
    checkOpen();
    block.bytes().write(bytes, navigator.getAndSetPosition(navigator.checkWrite(bytes.length)), bytes.length);
    return this;
  }

  @Override
  public BlockWriter writeByte(int b) {
    checkOpen();
    block.bytes().writeByte(navigator.getAndSetPosition(navigator.checkRead(Byte.BYTES)), b);
    return this;
  }

  @Override
  public BlockWriter writeChar(char c) {
    checkOpen();
    block.bytes().writeChar(navigator.getAndSetPosition(navigator.checkRead(Character.BYTES)), c);
    return this;
  }

  @Override
  public BlockWriter writeShort(short s) {
    checkOpen();
    block.bytes().writeShort(navigator.getAndSetPosition(navigator.checkRead(Short.BYTES)), s);
    return this;
  }

  @Override
  public BlockWriter writeInt(int i) {
    checkOpen();
    block.bytes().writeInt(navigator.getAndSetPosition(navigator.checkRead(Integer.BYTES)), i);
    return this;
  }

  @Override
  public BlockWriter writeLong(long l) {
    checkOpen();
    block.bytes().writeLong(navigator.getAndSetPosition(navigator.checkRead(Long.BYTES)), l);
    return this;
  }

  @Override
  public BlockWriter writeFloat(float f) {
    checkOpen();
    block.bytes().writeFloat(navigator.getAndSetPosition(navigator.checkRead(Float.BYTES)), f);
    return this;
  }

  @Override
  public BlockWriter writeDouble(double d) {
    checkOpen();
    block.bytes().writeDouble(navigator.getAndSetPosition(navigator.checkRead(Double.BYTES)), d);
    return this;
  }

  @Override
  public BlockWriter writeBoolean(boolean b) {
    checkOpen();
    block.bytes().writeBoolean(navigator.getAndSetPosition(navigator.checkRead(Byte.BYTES)), b);
    return this;
  }

  @Override
  public void close() {
    block.writers().release(this);
    block.release();
    open = false;
  }

}
