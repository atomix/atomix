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

import java.util.HashSet;
import java.util.Set;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.EntryReader;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryWriter;

/**
 * Cluster configuration entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=2, reader=ConfigurationEntry.Reader.class, writer=ConfigurationEntry.Writer.class)
public class ConfigurationEntry extends RaftEntry {
  private Set<String> members;

  private ConfigurationEntry() {
    super();
  }

  public ConfigurationEntry(long term, Set<String> members) {
    super(term);
    this.members = members;
  }

  /**
   * Returns a set of updated cluster members.
   * 
   * @return A set of cluster member addresses.
   */
  public Set<String> members() {
    return members;
  }

  @Override
  public String toString() {
    return String.format("ConfigurationEntry[term=%d, members=%s]", term(), members);
  }

  public static class Reader implements EntryReader<ConfigurationEntry> {
    @Override
    public ConfigurationEntry readEntry(Buffer buffer) {
      ConfigurationEntry entry = new ConfigurationEntry();
      entry.term = buffer.getLong();
      entry.members = buffer.getCollection(new HashSet<String>(), String.class);
      return entry;
    }
  }

  public static class Writer implements EntryWriter<ConfigurationEntry> {
    @Override
    public void writeEntry(ConfigurationEntry entry, Buffer buffer) {
      buffer.appendLong(entry.term);
      buffer.appendCollection(entry.members);
    }
  }

}
