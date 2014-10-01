/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log.impl;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryTypes;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogException;

import com.esotericsoftware.kryo.Kryo;

/**
 * Abstract base log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractLog implements Log {
  private final Class<? extends Entry> entryType;
  private final Map<Class<? extends Entry>, Integer> entryTypeMappings = new HashMap<>();
  private final Map<Integer, Class<? extends Entry>> entryClassMappings = new HashMap<>();
  protected final Kryo kryo;

  protected AbstractLog(Class<? extends Entry> entryType) {
    this.entryType = entryType;
    this.kryo = new Kryo();
    init();
  }

  /**
   * Initializes the log, loading entry type mappings.
   */
  private void init() {
    for (Class<? extends Entry> type : findEntryTypes(entryType).value()) {
      EntryType info = findEntryTypeInfo(type);
      entryTypeMappings.put(type, info.id());
      entryClassMappings.put(info.id(), type);
      try {
        kryo.register(type, info.serializer().newInstance(), info.id());
      } catch (InstantiationException | IllegalAccessException e) {
        throw new LogException(e);
      }
    }
  }

  /**
   * Finds entry type mappings from the base entry type.
   */
  private EntryTypes findEntryTypes(Class<?> clazz) {
    while (clazz != Object.class && clazz != null) {
      EntryTypes types = clazz.getAnnotation(EntryTypes.class);
      if (types != null) {
        return types;
      }
      clazz = clazz.getSuperclass();
    }
    throw new LogException("Invalid entry type. No type mappings found.");
  }

  /**
   * Finds entry type info for a specific entry type.
   */
  private EntryType findEntryTypeInfo(Class<?> clazz) {
    while (clazz != Object.class && clazz != null) {
      EntryType info = clazz.getAnnotation(EntryType.class);
      if (info != null) {
        return info;
      }
      clazz = clazz.getSuperclass();
    }
    throw new LogException("Invalid entry type. No type info found.");
  }

}
