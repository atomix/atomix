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
package net.kuujo.copycat.raft.log.entry;

import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.raft.log.StorageException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Type specific entry pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TypedEntryPool {
  private final Map<Class, ReferencePool<? extends RaftEntry<?>>> pools = new HashMap<>();

  /**
   * Acquires a specific entry type.
   */
  @SuppressWarnings("unchecked")
  public <T extends RaftEntry<T>> T acquire(Class<T> type, long index) {
    ReferencePool<T> pool = (ReferencePool<T>) pools.get(type);
    if (pool == null) {
      try {
        Constructor<T> c = type.getConstructor(ReferenceManager.class);
        c.setAccessible(true);
        pool = new ReferencePool<>((r) -> {
          try {
            return (T) c.newInstance(r);
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new StorageException(e);
          }
        });
      } catch (NoSuchMethodException e) {
        throw new SerializationException("failed to instantiate reference: must provide a single argument constructor", e);
      }
      pools.put(type, pool);
    }

    T entry = pool.acquire();
    entry.setIndex(index);
    return entry;
  }

}
