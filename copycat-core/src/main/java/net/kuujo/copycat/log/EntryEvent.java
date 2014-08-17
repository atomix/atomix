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

import net.kuujo.copycat.Event;

/**
 * Log entry event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EntryEvent implements Event {
  private final long index;
  private final Entry entry;

  public EntryEvent(long index, Entry entry) {
    this.index = index;
    this.entry = entry;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the log entry.
   *
   * @return The log entry.
   */
  public Entry entry() {
    return entry;
  }

}
