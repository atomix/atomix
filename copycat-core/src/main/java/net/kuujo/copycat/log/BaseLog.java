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
package net.kuujo.copycat.log;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.kuujo.copycat.internal.util.Assert;

import com.esotericsoftware.kryo.Kryo;

/**
 * Abstract base log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseLog implements Log {
  private final Class<? extends Entry> entryType;
  protected final Kryo kryo;

  protected BaseLog(Class<? extends Entry> entryType) {
    this.entryType = entryType;
    this.kryo = new Kryo();
    init();
  }
  
  @Override
  public List<Long> appendEntries(Entry... entries) {
    Assert.isNotNull(entries, "entries");
    assertIsOpen();
    return Arrays.stream(entries).map(entry -> appendEntry(entry)).collect(Collectors.toList());
  }
  
  @Override
  public List<Long> appendEntries(List<Entry> entries) {
    Assert.isNotNull(entries, "entries");
    assertIsOpen();
    return entries.stream().map(entry -> appendEntry(entry)).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return String.format("%s[size=%d]", getClass().getSimpleName(), size());
  }

  protected void assertIsOpen() {
    Assert.state(isOpen(), "The log is not currently open.");
  }
  
  protected void assertIsNotOpen() {
    Assert.state(!isOpen(), "The log is already open.");
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
   * Initializes the log, loading entry type mappings.
   */
  private void init() {
    for (Class<? extends Entry> type : findEntryTypes(entryType).value()) {
      EntryType info = findEntryTypeInfo(type);
      try {
        kryo.register(type, info.serializer().newInstance(), info.id());
      } catch (InstantiationException | IllegalAccessException e) {
        throw new LogException(e, "Failed to instantiate serializer %s", info.serializer().getName());
      }
    }
  }

}
