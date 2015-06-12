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
package net.kuujo.copycat.io.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * Pool of reference counted objects.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReferencePool<T extends ReferenceCounted<?>> implements ReferenceManager<T>, AutoCloseable {
  private final Function<ReferenceManager<T>, T> factory;
  private final Queue<T> pool = new ConcurrentLinkedQueue<>();

  public ReferencePool(Function<ReferenceManager<T>, T> factory) {
    if (factory == null)
      throw new NullPointerException("factory cannot be null");
    this.factory = factory;
  }

  /**
   * Acquires a reference.
   *
   * @return The acquired reference.
   */
  public T acquire() {
    T reference = pool.poll();
    if (reference == null) {
      reference = factory.apply(this);
    }
    reference.acquire();
    return reference;
  }

  @Override
  public void release(T reference) {
    pool.add(reference);
  }

  @Override
  public void close() {
    for (T reference : pool) {
      reference.close();
    }
  }

}
