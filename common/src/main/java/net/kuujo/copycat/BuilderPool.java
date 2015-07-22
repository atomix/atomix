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
package net.kuujo.copycat;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * Object builder pool.
 * <p>
 * Builders are frequently used throughout Copycat. In order to reduce garbage collection for objects that are frequently
 * constructed by builders, the builder pool recycles builders themselves. When a builder is {@link #acquire() acquired}
 * from a builder pool, an internal pool of existing builders will be {@link java.util.Queue#poll() polled} for existing
 * builders. If no builders are left in the pool, a new builder will be allocated. When {@link net.kuujo.copycat.Builder#close()}
 * is called the builder will be {@link #release(Builder) released} ba
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BuilderPool<T extends Builder<U>, U> {
  private final Function<BuilderPool<T, U>, T> factory;
  private final Queue<T> pool = new ConcurrentLinkedQueue<>();

  public BuilderPool(Function<BuilderPool<T, U>, T> factory) {
    if (factory == null)
      throw new NullPointerException("factory cannot be null");
    this.factory = factory;
  }

  /**
   * Acquires a builder from the pool.
   * <p>
   * If no builders remain in the pool, a new builder will be allocated via the {@link java.util.function.Function factory}
   * provided to this pool's constructor.
   *
   * @return The acquired builder.
   */
  public T acquire() {
    T builder = pool.poll();
    if (builder == null) {
      builder = factory.apply(this);
    }
    builder.reset();
    return builder;
  }

  /**
   * Acquires a builder from the pool.
   * <p>
   * If no builders remain in the pool, a new builder will be allocated via the {@link java.util.function.Function factory}
   * provided to this pool's constructor.
   *
   * @param object The object to build.
   * @return The acquired builder.
   */
  public T acquire(U object) {
    T builder = pool.poll();
    if (builder == null) {
      builder = factory.apply(this);
    }
    builder.reset(object);
    return builder;
  }

  /**
   * Releases the given builder back to the pool.
   * <p>
   * The builder will be stored in an internal {@link java.util.Queue} to be recycled on the next call to {@link #acquire()}.
   *
   * @param builder The builder to release back to the pool.
   */
  public void release(T builder) {
    pool.add(builder);
  }

}
