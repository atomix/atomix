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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
  static <T extends Builder<U>, U extends Operation<?>> T builder(Class<T> type) {
    return (T) Builder.BUILDERS.get().computeIfAbsent(type, t -> {
      try {
        return type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("failed to instantiate builder: " + type, e);
      }
    });
  }

  /**
   * Operation builder.
   */
  static abstract class Builder<T extends Operation<?>> implements net.kuujo.copycat.Builder<T> {
    private static final ThreadLocal<Map<Class<? extends Builder>, Builder>> BUILDERS = new ThreadLocal<Map<Class<? extends Builder>, Builder>>() {
      @Override
      protected Map<Class<? extends Builder>, Builder> initialValue() {
        return new HashMap<>();
      }
    };

    private final T operation;

    protected Builder(T operation) {
      this.operation = operation;
    }

    @Override
    public T build() {
      return operation;
    }
  }

}
