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

import net.kuujo.copycat.log.EntryType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Cluster configuration entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=4, serializer=ConfigurationEntry.Serializer.class)
public class ConfigurationEntry extends RaftEntry {
  private Set<String> cluster;

  private ConfigurationEntry() {
    super();
  }

  public ConfigurationEntry(long term, Set<String> cluster) {
    super(term);
    this.cluster = cluster;
  }

  /**
   * Returns a set of updated cluster members.
   * 
   * @return A set of cluster member addresses.
   */
  public Set<String> cluster() {
    return cluster;
  }

  @Override
  public String toString() {
    return String.format("ConfigurationEntry[term=%d, cluster=%s]", term(), cluster);
  }

  /**
   * Configuration entry serializer.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<ConfigurationEntry> {
    @Override
    @SuppressWarnings("unchecked")
    public ConfigurationEntry read(Kryo kryo, Input input, Class<ConfigurationEntry> type) {
      ConfigurationEntry entry = new ConfigurationEntry();
      entry.term = input.readLong();
      entry.cluster = kryo.readObject(input, HashSet.class);
      return entry;
    }
    @Override
    public void write(Kryo kryo, Output output, ConfigurationEntry entry) {
      output.writeLong(entry.term);
      kryo.writeObject(output, entry.cluster);
    }
  }

}
