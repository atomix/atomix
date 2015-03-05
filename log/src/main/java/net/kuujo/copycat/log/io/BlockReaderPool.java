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

import net.kuujo.copycat.log.io.util.ReferenceManager;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Block reader pool.
 * <p>
 * The reader pool reduces garbage produced by frequent reads by tracking references to existing readers and recycling
 * readers once they're closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class BlockReaderPool implements ReferenceManager<BlockReader> {
  private final Block block;
  private final Queue<BlockReader> pool = new ArrayDeque<>(1024);

  public BlockReaderPool(Block block) {
    if (block == null)
      throw new NullPointerException("block cannot be null");
    this.block = block;
  }

  /**
   * Acquires a new multi buffer reader.
   */
  public BlockReader acquire() {
    BlockReader reader = pool.poll();
    if (reader == null) {
      synchronized (pool) {
        reader = pool.poll();
        if (reader == null) {
          reader = new BlockReader(block, 0, 0);
        }
      }
    }
    return reader;
  }

  @Override
  public void release(BlockReader reference) {
    pool.add(reference);
  }

}
