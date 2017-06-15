/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.protocols.raft.storage.entry;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores a cluster configuration.
 * <p>
 * The {@code ConfigurationEntry} stores information relevant to a single cluster configuration change.
 * Configuration change entries store a collection of {@link RaftMember members} which each represent a
 * server in the cluster. Each time the set of members changes or a property of a single member changes,
 * a new {@code ConfigurationEntry} must be logged for the configuration change.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigurationEntry extends TimestampedEntry<ConfigurationEntry> {
  private final Collection<RaftMember> members;

  public ConfigurationEntry(long timestamp, Collection<RaftMember> members) {
    super(timestamp);
    this.members = checkNotNull(members, "members cannot be null");
  }

  @Override
  public Type<ConfigurationEntry> type() {
    return Type.CONFIGURATION;
  }

  /**
   * Returns the members.
   *
   * @return The members.
   */
  public Collection<RaftMember> members() {
    return members;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("members", members)
        .toString();
  }

  /**
   * Configuration entry serializer.
   */
  public static class Serializer implements TimestampedEntry.Serializer<ConfigurationEntry> {
    @Override
    public void writeObject(BufferOutput output, ConfigurationEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeInt(entry.members.size());
      for (RaftMember member : entry.members) {
        output.writeString(member.id().id());
        output.writeByte(member.type().ordinal());
        output.writeByte(member.status().ordinal());
        output.writeLong(member.updated().toEpochMilli());
      }
    }

    @Override
    public ConfigurationEntry readObject(BufferInput input, Class<ConfigurationEntry> type) {
      long timestamp = input.readLong();
      int size = input.readInt();
      List<RaftMember> members = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        NodeId id = NodeId.nodeId(input.readString());
        RaftMember.Type memberType = RaftMember.Type.values()[input.readByte()];
        RaftMember.Status memberStatus = RaftMember.Status.values()[input.readByte()];
        Instant updated = Instant.ofEpochMilli(input.readLong());
        members.add(new DefaultRaftMember(id, memberType, memberStatus, updated));
      }
      return new ConfigurationEntry(timestamp, members);
    }
  }
}