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

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Block writer pool.
 * <p>
 * The writer pool reduces garbage produced by frequent reads by tracking references to existing writers and recycling
 * writers once they're closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class BlockWriterPool implements ReferenceManager<BlockWriter> {
  private final Block block;
  private final Queue<BlockWriter> pool = new ArrayDeque<>(1024);

  public BlockWriterPool(Block block) {
    if (block == null)
      throw new NullPointerException("block cannot be null");
    this.block = block;
  }

  /**
   * Acquires a new multi buffer writer.
   */
  public BlockWriter acquire() {
    BlockWriter writer = pool.poll();
    if (writer == null) {
      synchronized (pool) {
        writer = pool.poll();
        if (writer == null) {
          writer = new BlockWriter(block, 0, 0);
        }
      }
    }
    return writer;
  }

  @Override
  public void release(BlockWriter reference) {
    pool.add(reference);
  }

}
