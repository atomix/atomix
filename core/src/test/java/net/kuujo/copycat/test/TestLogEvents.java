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
package net.kuujo.copycat.test;

import net.kuujo.copycat.log.Entry;

/**
 * Test log events.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestLogEvents {
  private final TestLog log;

  public TestLogEvents(TestLog log) {
    this.log = log;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param entry The entry.
   * @param callback The event callback.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(Entry entry, Runnable callback) {
    log.addEntryListener((i, e) -> e.equals(entry), callback);
    return this;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param index The entry index.
   * @param callback The event callback.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(long index, Runnable callback) {
    log.addEntryListener((i, e) -> i == index, callback);
    return this;
  }

  /**
   * Listens for an entry being appended to the log.
   *
   * @param entryType The entry type.
   * @param callback The event callback.
   * @return The event object.
   */
  public TestLogEvents appendedEntry(Class<? extends Entry> entryType, Runnable callback) {
    log.addEntryListener((i, e) -> entryType.isAssignableFrom(e.getClass()), callback);
    return this;
  }

  /**
   * Listens for the log to be compacted.
   *
   * @param callback An event callback.
   * @return The event object.
   */
  public TestLogEvents compacted(Runnable callback) {
    log.addCompactListener(callback);
    return this;
  }

}
