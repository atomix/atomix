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
 * Storage block.
 * <p>
 * Storage blocks represent a fixed section of the underlying persistence layer. Each block is backed by a {@link Bytes}
 * object to which bytes are written directly. Blocks maintain their own read/write indexes. Additionally, blocks provide
 * {@link BufferReader} and {@link BufferWriter} which read from and write to the block respectively and maintain positions
 * and limits separate from those of the block itself.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Block extends CheckedBuffer implements ReferenceCounted<Block> {
  private final AtomicInteger references = new AtomicInteger();
  private final int index;
  private final ReferenceManager<Block> manager;
  private final BufferReaderPool<Block, BlockReader> readerPool;
  private final BufferWriterPool<Block, BlockWriter> writerPool;
  private boolean open;

  public Block(int index, Bytes bytes) {
    this(index, bytes, null);
  }

  public Block(int index, Bytes bytes, ReferenceManager<Block> manager) {
    super(bytes);
    if (index < 0)
      throw new IllegalArgumentException("index cannot be negative");
    this.index = index;
    this.manager = manager;
    this.readerPool = new BufferReaderPool<>(this, BlockReader::new);
    this.writerPool = new BufferWriterPool<>(this);
  }

  /**
   * Returns the block index.
   *
   * @return The block index.
   */
  public int index() {
    return index;
  }

  /**
   * Opens the block.
   */
  Block open() {
    clear();
    open = true;
    references.incrementAndGet();
    return this;
  }

  /**
   * Checks whether the block is open.
   */
  private void checkOpen() {
    if (!open)
      throw new IllegalStateException("block not open");
    if (references.get() == 0)
      throw new IllegalReferenceException("block has no active references");
  }

  @Override
  public Block acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0)
      manager.release(this);
  }

  @Override
  public int references() {
    return references.get();
  }

  /**
   * Returns a block reader with the current block position and limit.
   * <p>
   * The block reader is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other readers returned by this block. The block
   * will maintain a reference to the returned reader. Once bytes have been written to the block, close the reader
   * via {@link BlockReader#close()} to release the reader back to the block. This allows blocks to recycle existing
   * readers with {@link net.kuujo.copycat.log.io.BufferReaderPool} rather than creating a new block for each new write.
   *
   * @return The block reader.
   */
  public BlockReader reader() {
    return (BlockReader) readerPool.acquire().reset(position(), limit());
  }

  /**
   * Returns a block reader with the current block limit.
   * <p>
   * The block reader is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other readers returned by this block. The block
   * will maintain a reference to the returned reader. Once bytes have been written to the block, close the reader
   * via {@link BlockReader#close()} to release the reader back to the block. This allows blocks to recycle existing
   * readers with {@link net.kuujo.copycat.log.io.BufferReaderPool} rather than creating a new block for each new write.
   *
   * @param offset The offset from which to begin reading.
   * @return The block reader.
   */
  public BlockReader reader(long offset) {
    return (BlockReader) readerPool.acquire().reset(offset, limit());
  }

  /**
   * Returns a block reader with the given offset and length.
   * <p>
   * The block reader is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other readers returned by this block. The block
   * will maintain a reference to the returned reader. Once bytes have been written to the block, close the reader
   * via {@link BlockReader#close()} to release the reader back to the block. This allows blocks to recycle existing
   * readers with {@link BufferReaderPool} rather than creating a new block for each new write.
   *
   * @param offset The offset from which to begin reading.
   * @param length The maximum number of bytes to be read by the returned reader.
   * @return The block reader.
   */
  public BlockReader reader(long offset, long length) {
    return (BlockReader) readerPool.acquire().reset(offset, offset + length);
  }

  /**
   * Returns a block writer with the current block position and limit.
   * <p>
   * The block writer is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other writers returned by this block. The block
   * will maintain a reference to the returned writer. Once bytes have been written to the block, close the writer
   * via {@link BlockWriter#close()} to release the writer back to the block. This allows blocks to recycle existing
   * writers with {@link BufferWriterPool} rather than creating a new block for each new write.
   *
   * @return The block writer.
   */
  public BlockWriter writer() {
    return (BlockWriter) writerPool.acquire().reset(position(), limit());
  }

  /**
   * Returns a block writer with the current block limit.
   * <p>
   * The block writer is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other writers returned by this block. The block
   * will maintain a reference to the returned writer. Once bytes have been written to the block, close the writer
   * via {@link BlockWriter#close()} to release the writer back to the block. This allows blocks to recycle existing
   * writers with {@link BufferWriterPool} rather than creating a new block for each new write.
   *
   * @param offset The offset from which to begin writing.
   * @return The block writer.
   */
  public BlockWriter writer(long offset) {
    return (BlockWriter) writerPool.acquire().reset(offset, limit());
  }

  /**
   * Returns a block writer with the given offset and length.
   * <p>
   * The block writer is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other writers returned by this block. The block
   * will maintain a reference to the returned writer. Once bytes have been written to the block, close the writer
   * via {@link BlockWriter#close()} to release the writer back to the block. This allows blocks to recycle existing
   * writers with {@link BufferWriterPool} rather than creating a new block for each new write.
   *
   * @param offset The offset from which to begin writing.
   * @param length The maximum number of bytes to be write by the returned writer.
   * @return The block writer.
   */
  public BlockWriter writer(long offset, long length) {
    return (BlockWriter) writerPool.acquire().reset(offset, offset + length);
  }

  @Override
  public void close() {
    open = false;
    release();
  }

}
