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

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Block writer pool.
 * <p>
 * The writer pool reduces garbage produced by frequent writes by tracking references to existing writers and recycling
 * writers once they're closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBlockWriterPool<T extends Block & Referenceable<T>> implements ReferenceManager<ReusableBlockWriter<T>> {
  private final T buffer;
  private final Queue<ReusableBlockWriter<T>> pool = new ArrayDeque<>(1024);

  public ReusableBlockWriterPool(T buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
  }

  /**
   * Acquires a new multi buffer writer.
   */
  public ReusableBlockWriter<T> acquire() {
    ReusableBlockWriter<T> writer = pool.poll();
    if (writer == null) {
      synchronized (pool) {
        writer = pool.poll();
        if (writer == null) {
          writer = new ReusableBlockWriter<>(buffer, this);
        }
      }
    }
    writer.clear();
    return writer;
  }

  @Override
  public void release(ReusableBlockWriter<T> reference) {
    pool.add(reference);
    reference.buffer().release();
  }

}
