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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.BuilderPool;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * State machine operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Operation<T> extends Serializable {

  /**
   * Returns a cached instance of the given builder.
   *
   * @param type The builder type.
   * @return The builder.
   */
  @SuppressWarnings("unchecked")
  static <T extends Builder> T builder(Class<T> type, Function<BuilderPool, T> factory) {
    // We run into strange reflection issues when using a lambda here, so just use an old style closure instead.
    BuilderPool pool = Builder.POOLS.computeIfAbsent(type, t -> new BuilderPool(factory));
    T builder = (T) pool.acquire();
    builder.reset(builder.create());
    return builder;
  }

  /**
   * Operation builder.
   */
  static abstract class Builder<T extends Builder<T, U, V>, U extends Operation<V>, V> extends net.kuujo.copycat.Builder<U> {
    static final Map<Class<? extends Builder>, BuilderPool> POOLS = new ConcurrentHashMap<>();

    protected U operation;

    protected Builder(BuilderPool<T, U> pool) {
      super(pool);
    }

    /**
     * Creates a new operation instance.
     *
     * @return A new operation instance.
     */
    protected abstract U create();

    @Override
    protected void reset(U operation) {
      this.operation = operation;
    }

    @Override
    public U build() {
      close();
      return operation;
    }
  }

}
