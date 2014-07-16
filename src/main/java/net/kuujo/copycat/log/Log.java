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

import java.util.List;

import org.vertx.java.core.Handler;

/**
 * A replicated log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Log {

  /**
   * A log type.
   *
   * @author Jordan Halterman
   */
  public static enum Type {

    /**
     * An in-memory log type.
     */
    MEMORY("memory", MemoryLog.class),

    /**
     * A file-based log type.
     */
    FILE("file", FileLog.class);

    private final String name;
    private final Class<? extends Log> type;

    private Type(String name, Class<? extends Log> type) {
      this.name = name;
      this.type = type;
    }

    /**
     * Returns the log type name.
     *
     * @return The log type name.
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the log type.
     *
     * @return The log type.
     */
    public Class<? extends Log> getType() {
      return type;
    }

    @Override
    public String toString() {
      return name;
    }

    /**
     * Returns a log type from a string.
     *
     * @param name A string log type name.
     * @return A log type.
     * @throws IllegalArgumentException If the log type name is invalid.
     */
    public static Type parse(String name) {
      switch (name) {
        case "memory":
          return MEMORY;
        case "file":
          return FILE;
        default:
          throw new IllegalArgumentException("Invalid log type " + name);
      }
    }

  }

  /**
   * Opens the log.
   */
  void open();

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum log size.
   * @return The log instance.
   */
  Log setMaxSize(long maxSize);

  /**
   * Returns the maximum log size.
   *
   * @return The maximum log size.
   */
  long getMaxSize();

  /**
   * Sets a full handler on the log.
   *
   * @param handler A handler to be called when the log is full.
   * @return The log instance.
   */
  Log fullHandler(Handler<Void> handler);

  /**
   * Sets a drain handler on the log.
   *
   * @param handler A handler to be called when the log is drained.
   * @return The log instance.
   */
  Log drainHandler(Handler<Void> handler);

  /**
   * Appends an entry to the log.
   * 
   * @param entry The entry to append.
   * @return The index at which the entry was appended.
   */
  long appendEntry(Entry entry);

  /**
   * Returns a boolean indicating whether the log has an entry at the given
   * index.
   * 
   * @param index The index to check.
   * @return Indicates whether the log has an entry at the given index.
   */
  boolean containsEntry(long index);

  /**
   * Returns the entry at the given index.
   * 
   * @param index The index from which to get the entry.
   * @return A log entry.
   */
  Entry getEntry(long index);

  /**
   * Sets an entry at a specific index.
   *
   * @param index The index at which to set the entry.
   * @param entry The entry to set.
   * @return The log instance.
   */
  Log setEntry(long index, Entry entry);

  /**
   * Returns the first log index.
   *
   * @return
   *   The first log index.
   */
  long firstIndex();

  /**
   * Returns the first log entry.
   *
   * @return The first log entry.
   */
  Entry firstEntry();

  /**
   * Returns the last log index.
   *
   * @return
   *   The last log index.
   */
  long lastIndex();

  /**
   * Returns the last log entry.
   *
   * @return The last log entry.
   */
  Entry lastEntry();

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  List<Entry> getEntries(long start, long end);

  /**
   * Removes all entries before the given index.
   * 
   * @param index The index before which to remove entries.
   */
  void removeBefore(long index);

  /**
   * Removes all entries after the given index.
   * 
   * @param index The index after which to remove entries.
   */
  void removeAfter(long index);

  /**
   * Closes the log.
   */
  void close();

  /**
   * Deletes the log.
   */
  void delete();

}
