/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.utils.misc.TimestampPrinter;

import java.util.Collection;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Stores a cluster configuration.
 * <p>
 * The {@code ConfigurationEntry} stores information relevant to a single cluster configuration change.
 * Configuration change entries store a collection of {@link RaftMember members} which each represent a
 * server in the cluster. Each time the set of members changes or a property of a single member changes,
 * a new {@code ConfigurationEntry} must be logged for the configuration change.
 */
public class ConfigurationEntry extends TimestampedEntry {
  protected final Collection<RaftMember> members;

  public ConfigurationEntry(long term, long timestamp, Collection<RaftMember> members) {
    super(term, timestamp);
    this.members = members;
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
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .add("members", members)
        .toString();
  }
}