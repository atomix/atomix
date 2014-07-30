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

import java.util.Set;

import net.kuujo.copycat.log.Entry;

/**
 * State machine snapshot log entry.<p>
 *
 * The snapshot entry is a log entry containing a snapshot of the
 * local state machine's state at a given moment in time. Rather than
 * storing snapshots in a separate snapshot file, CopyCat logs snapshots
 * in order to more easily facilitate replication of snapshots to
 * replicas that are too far out of sync with the leader's log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotEntry extends Entry {
  private static final long serialVersionUID = -5487522806426222419L;
  private Set<String> cluster;
  private byte[] data;
  private boolean complete;

  public SnapshotEntry() {
    super();
  }

  public SnapshotEntry(long term, Set<String> cluster, byte[] data, boolean complete) {
    super(term);
    this.cluster = cluster;
    this.data = data;
  }

  /**
   * Returns the snapshot cluster configuration.
   *
   * @return The snapshot cluster configuration.
   */
  public Set<String> cluster() {
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

  /**
   * Returns a boolean indicating whether the snapshot is complete.
   *
   * @return Indicates whether the snapshot is complete.
   */
  public boolean complete() {
    return complete;
  }

  @Override
  public String toString() {
    return String.format("Snapshot[%s]", data);
  }

}
