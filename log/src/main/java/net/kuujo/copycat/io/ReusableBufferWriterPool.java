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

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Buffer writer pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBufferWriterPool<T extends Buffer & Referenceable<T>> implements ReferenceManager<ReusableBufferWriter<T>> {
  private final T buffer;
  private final Queue<ReusableBufferWriter<T>> pool = new ArrayDeque<>(1024);

  public ReusableBufferWriterPool(T buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
  }

  /**
   * Acquires a new multi buffer writer.
   */
  public ReusableBufferWriter<T> acquire() {
    ReusableBufferWriter<T> writer = pool.poll();
    if (writer == null) {
      synchronized (pool) {
        writer = pool.poll();
        if (writer == null) {
          writer = new ReusableBufferWriter<>(buffer, this);
        }
      }
    }
    writer.reset();
    return writer;
  }

  @Override
  public void release(ReusableBufferWriter<T> reference) {
    pool.add(reference);
    reference.buffer().release();
  }

}
