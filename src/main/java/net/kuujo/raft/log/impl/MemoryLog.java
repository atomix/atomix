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
package net.kuujo.raft.log.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.kuujo.raft.Command;
import net.kuujo.raft.impl.DefaultCommand;
import net.kuujo.raft.log.CommandEntry;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.Log;
import net.kuujo.raft.log.Entry.Type;

/**
 * A default log implementation.
 *
 * @author Jordan Halterman
 */
public class MemoryLog implements Log {
  private final TreeMap<Long, Entry> log = new TreeMap<>();
  private final Map<String, Long> commands = new HashMap<>();
  private final List<Long> freed = new ArrayList<>();
  private long floor;

  @Override
  public void init() {
    
  }

  @Override
  public long appendEntry(Entry entry) {
    long index = lastIndex() + 1;
    log.put(index, entry);
    if (entry.type().equals(Type.COMMAND)) {
      Command command = ((CommandEntry) entry).command();
      commands.put(command instanceof DefaultCommand ? ((DefaultCommand) command).setLog(this).id() : command.id(), index);
    }
    return index;
  }

  @Override
  public boolean containsEntry(long index) {
    return log.containsKey(index);
  }

  @Override
  public Entry entry(long index) {
    return log.get(index);
  }

  @Override
  public long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : -1;
  }

  @Override
  public long firstTerm() {
    return !log.isEmpty() ? log.firstEntry().getValue().term() : -1;
  }

  @Override
  public Entry firstEntry() {
    return !log.isEmpty() ? log.firstEntry().getValue() : null;
  }

  @Override
  public long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : -1;
  }

  @Override
  public long lastTerm() {
    return !log.isEmpty() ? log.lastEntry().getValue().term() : -1;
  }

  @Override
  public Entry lastEntry() {
    return !log.isEmpty() ? log.lastEntry().getValue() : null;
  }

  @Override
  public List<Entry> entries(long start, long end) {
    List<Entry> entries = new ArrayList<>();
    for (Map.Entry<Long, Entry> entry : log.subMap(start, end).entrySet()) {
      entries.add(entry.getValue());
    }
    return entries;
  }

  @Override
  public Entry removeEntry(long index) {
    return log.remove(index);
  }

  @Override
  public Log removeBefore(long index) {
    log.headMap(index);
    return this;
  }

  @Override
  public Log removeAfter(long index) {
    log.tailMap(index);
    return this;
  }

  @Override
  public long floor() {
    return floor;
  }

  @Override
  public Log floor(long index) {
    floor = index;

    // Sort the freed list.
    Collections.sort(freed);

    // Iterate over indexes in the freed list.
    boolean removed = false;
    for (long item : freed) {
      if (item < floor) {
        commands.remove(((CommandEntry) log.remove(item)).command().id());
        removed = true;
      }
    }

    // If any items were removed from the log then rewrite log entries to the
    // head of the log.
    if (removed) {
      rewrite();
    }
    return this;
  }

  @Override
  public void free(String command) {
    if (commands.containsKey(command)) {
      long index = commands.get(command);
      if (index < floor) {
        log.remove(index);
        commands.remove(command);
        rewrite();
      }
      else {
        freed.add(index);
      }
    }
  }

  /**
   * Rewrites all entries to the head of the log.
   */
  private void rewrite() {
    long lastIndex = log.lastKey();
    long firstIndex = log.firstKey();
    List<Long> empty = new ArrayList<>();
    for (long i = lastIndex; i >= firstIndex; i--) {
      if (!log.containsKey(i)) {
        empty.add(i);
      }
      else if (empty.size() > 0) {
        log.put(empty.remove(0), log.remove(i));
        empty.add(i);
      }
    }
  }

}
