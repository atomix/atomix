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

import java.util.Arrays;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.log.EntryType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Snapshot log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("rawtypes")
@EntryType(id=5, serializer=SnapshotEntry.Serializer.class)
public class SnapshotEntry extends CopycatEntry {
  private Cluster cluster;
  private byte[] data;

  private SnapshotEntry() {
    super();
  }

  public SnapshotEntry(long term, Cluster cluster, byte[] data) {
    super(term);
    this.cluster = cluster;
    this.data = data;
  }

  /**
   * Returns the snapshot cluster configuration.
   *
   * @return The snapshot cluster configuration.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SnapshotEntry) {
      SnapshotEntry entry = (SnapshotEntry) object;
      return term == entry.term && cluster.equals(entry.cluster) && Arrays.equals(data, entry.data);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + cluster.hashCode();
    hashCode = 37 * hashCode + Arrays.hashCode(data);
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("SnapshotEntry[term=%d, config=%s, data=...]", term, cluster);
  }

  /**
   * Snapshot entry serializer.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<SnapshotEntry> {
    @Override
    public SnapshotEntry read(Kryo kryo, Input input, Class<SnapshotEntry> type) {
      SnapshotEntry entry = new SnapshotEntry();
      entry.term = input.readLong();
      entry.cluster = (Cluster) kryo.readClassAndObject(input);
      int length = input.readInt();
      entry.data = new byte[length];
      input.readBytes(entry.data);
      return entry;
    }
    @Override
    public void write(Kryo kryo, Output output, SnapshotEntry entry) {
      output.writeLong(entry.term);
      kryo.writeClassAndObject(output, entry.cluster);
      output.writeInt(entry.data.length);
      output.writeBytes(entry.data);
    }
  }

}
