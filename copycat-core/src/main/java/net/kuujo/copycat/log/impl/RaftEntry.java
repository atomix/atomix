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

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.EntryTypes;

/**
 * Raft log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryTypes({
  NoOpEntry.class,
  CommandEntry.class,
  ConfigurationEntry.class,
  SnapshotEntry.class
})
public class RaftEntry implements Entry {
  protected long term;

  protected RaftEntry() {
  }

  protected RaftEntry(long term) {
    this.term = term;
  }

  /**
   * Returns the log entry term.
   * 
   * @return The log entry term.
   */
  public long term() {
    return term;
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isAssignableFrom(object.getClass()) && ((RaftEntry) object).term() == term;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%s]", getClass().getSimpleName(), term);
  }

}
