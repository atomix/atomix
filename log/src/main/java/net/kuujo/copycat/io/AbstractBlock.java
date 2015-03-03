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

import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.io.util.Referenceable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract block implementation.
 * <p>
 * This is a base block implementation that handles common methods for all blocks. It handles the navigability
 * of the block and delegates read/write calls to the subclass's respective index specific read/write methods.
 * <p>
 * Blocks that extend {@code AbstractBlock} are reference counted. Reference counting is used to keep track of when the
 * block is in use. When {@link AbstractBlock#reader()} or {@link AbstractBlock#writer()} is called, a new reference to
 * the block is created. Once the returned {@link BlockReader} or {@link BlockWriter} is closed, the reference is
 * released. Once all references to the block have been released - including the reference to tbe block itself via
 * {@link Block#close()} - the storage instance which allocated the block will be allowed to free the block. Note that
 * the behavior of storage implementations when block references are released varies widely across storage implementations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractBlock<T extends AbstractBlock<T>> extends BytesNavigator<Block> implements Block, Referenceable<T> {
  private final AtomicInteger referenceCounter = new AtomicInteger();
  private final ReferenceManager<T> referenceManager;
  private final ReusableBlockReaderPool<T> readerPool;
  private final ReusableBlockWriterPool<T> writerPool;
  private final int index;

  @SuppressWarnings("unchecked")
  public AbstractBlock(int index, long capacity, ReferenceManager<T> manager) {
    super(capacity);
    if (index < 0)
      throw new IndexOutOfBoundsException("block index cannot be negative");
    if (manager == null)
      throw new NullPointerException("manager cannot be null");
    this.referenceManager = manager;
    this.readerPool = new ReusableBlockReaderPool<>((T) this);
    this.writerPool = new ReusableBlockWriterPool<>((T) this);
    this.index = index;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T acquire() {
    referenceCounter.incrementAndGet();
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void release() {
    if (referenceCounter.decrementAndGet() == 0)
      referenceManager.release((T) this);
  }

  @Override
  public int references() {
    return referenceCounter.get();
  }

  @Override
  public BlockReader reader() {
    return readerPool.acquire();
  }

  @Override
  public BlockWriter writer() {
    return writerPool.acquire();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    release();
  }

}
