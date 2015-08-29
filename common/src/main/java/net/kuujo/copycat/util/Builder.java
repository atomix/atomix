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
package net.kuujo.copycat.util;

/**
 * Object builder.
 * <p>
 * This is a base interface for building objects in Copycat.
 * <p>
 * Throughout Copycat, builders are used to build a variety of objects. Builders are designed to be
 * {@link BuilderPool pooled} and reused in order to reduce GC pressure. When {@link #build()} or
 * {@link #close()} is called on a builder, the builder will be released back to the {@link BuilderPool}
 * that created it.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Builder<T> implements AutoCloseable {
  protected final BuilderPool pool;

  protected Builder() {
    this(null);
  }

  /**
   * @throws NullPointerException if {@code pool} is null
   */
  protected Builder(BuilderPool pool) {
    this.pool = pool;
  }

  /**
   * Resets the builder.
   * <p>
   * Builders should override this method to reset internal builder state for builders pooled via
   * {@link BuilderPool}. Each time a new builder is {@link BuilderPool#acquire() acquired} from a
   * pool, this method will be called to reset the internal builder state.
   */
  protected void reset() {

  }

  /**
   * Resets the builder.
   * <p>
   * Builders should override this method to reset internal builder state for builders pooled via
   * {@link BuilderPool}. Each time a new builder is {@link BuilderPool#acquire(Object) acquired} from a
   * pool, this method will be called to reset the internal builder state.
   * 
   * @throws NullPointerException if {@code object} is null
   */
  protected void reset(T object) {

  }

  /**
   * Builds the object.
   * <p>
   * The returned object may be a new instance of the built class or a recycled instance, depending on the semantics
   * of the builder implementation. Users should never assume that a builder allocates a new instance.
   *
   * @return The built object.
   */
  public abstract T build();

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    if (pool != null) {
      pool.release(this);
    }
  }

}
