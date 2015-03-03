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

import net.kuujo.copycat.io.util.Allocator;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.io.IOException;

/**
 * Native memory storage.
 * <p>
 * This storage implementation allocates off-heap memory and provides direct access via {@link sun.misc.Unsafe}. It's
 * important to note that once a block has been acquired for the first time via {@link NativeStorage#acquire(int)}, the
 * memory allocated for that block will remain available for the lifetime of the storage instance (until
 * {@link NativeStorage#close()} is called).
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeStorage implements Storage, ReferenceManager<NativeBlock> {
  private final Allocator allocator;
  private final ReusableBlockPool<NativeBlock> pool;
  private final long blockSize;

  public NativeStorage(Allocator allocator, long blockSize) {
    if (allocator == null)
      throw new NullPointerException("memory allocator cannot be null");
    if (blockSize <= 0)
      throw new IllegalArgumentException("block size must be positive");
    this.allocator = allocator;
    this.blockSize = blockSize;
    this.pool = new ReusableBlockPool<>(this::createBlock);
  }

  /**
   * Creates a new block.
   */
  private NativeBlock createBlock(int index) {
    return new NativeBlock(index, allocator.allocate(blockSize), this);
  }

  @Override
  public Block acquire(int index) {
    return pool.acquire(index);
  }

  @Override
  public void release(NativeBlock reference) {
    reference.reset();
  }

  @Override
  public void close() throws IOException {
    pool.blocks().forEach(b -> b.memory().free());
  }

}
