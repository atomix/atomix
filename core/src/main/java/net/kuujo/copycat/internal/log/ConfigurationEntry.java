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
package net.kuujo.copycat.internal.log;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.log.EntryType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Cluster configuration entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id = 4, serializer = ConfigurationEntry.Serializer.class)
public class ConfigurationEntry extends CopycatEntry {
  private Cluster cluster;

  private ConfigurationEntry() {
    super();
  }

  public ConfigurationEntry(long term, Cluster cluster) {
    super(term);
    this.cluster = cluster;
  }

  /**
   * Returns a set of updated cluster members.
   * 
   * @return A set of cluster member addresses.
   */
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConfigurationEntry) {
      ConfigurationEntry entry = (ConfigurationEntry) object;
      return term == entry.term && cluster.equals(entry.cluster);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int) (term ^ (term >>> 32));
    hashCode = 37 * hashCode + cluster.hashCode();
    return hashCode;
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
    public ConfigurationEntry read(Kryo kryo, Input input, Class<ConfigurationEntry> type) {
      ConfigurationEntry entry = new ConfigurationEntry();
      entry.term = input.readLong();
      entry.cluster = (Cluster) kryo.readClassAndObject(input);
      return entry;
    }

    @Override
    public void write(Kryo kryo, Output output, ConfigurationEntry entry) {
      output.writeLong(entry.term);
      kryo.writeClassAndObject(output, entry.cluster);
    }
  }
}
