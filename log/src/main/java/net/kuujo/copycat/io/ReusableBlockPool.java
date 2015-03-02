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

import java.util.*;

/**
 * Multi-block pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBlockPool<T extends Block & Referenceable<T>> implements ReferenceManager<ReusableBlock<T>> {
  private final BlockFactory<T> factory;
  private final Map<Integer, T> blocks = new HashMap<>();
  private final Map<Integer, Queue<ReusableBlock<T>>> pools = new HashMap<>();

  public ReusableBlockPool(BlockFactory<T> factory) {
    this.factory = factory;
  }

  /**
   * Gets a pool for the given index.
   */
  private Queue<ReusableBlock<T>> getPool(int index) {
    Queue<ReusableBlock<T>> pool = pools.get(index);
    if (pool == null) {
      synchronized (pools) {
        pool = pools.get(index);
        if (pool == null) {
          pool = new ArrayDeque<>(1024);
          pools.put(index, pool);
        }
      }
    }
    return pool;
  }

  /**
   * Acquires the block for the given index.
   */
  public ReusableBlock<T> acquire(int index) {
    Queue<ReusableBlock<T>> pool = getPool(index);
    ReusableBlock<T> block = pool.poll();
    if (block == null) {
      synchronized (pool) {
        block = pool.poll();
        if (block == null) {
          T buffer = blocks.get(index);
          if (buffer == null) {
            buffer = factory.createBlock(index);
            blocks.put(index, buffer);
          }
          block = new ReusableBlock<>(buffer, this);
        }
      }
    }
    block.reset();
    return block;
  }

  @Override
  public void release(ReusableBlock<T> reference) {
    getPool(reference.index()).add(reference);
    reference.buffer().release();
  }

  /**
   * Returns a collection of blocks in the pool.
   */
  Collection<T> blocks() {
    return blocks.values();
  }

}
