/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.storage.snapshot.Snapshot;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Pending snapshot.
 */
public class PendingSnapshot {
  private final Snapshot snapshot;
  private long nextOffset;

  public PendingSnapshot(Snapshot snapshot) {
    this.snapshot = snapshot;
  }

  /**
   * Returns the pending snapshot.
   *
   * @return the pending snapshot
   */
  public Snapshot snapshot() {
    return snapshot;
  }

  /**
   * Returns and increments the next snapshot offset.
   *
   * @return the next snapshot offset
   */
  public long nextOffset() {
    return nextOffset;
  }

  /**
   * Increments the next snapshot offset.
   */
  public void incrementOffset() {
    nextOffset++;
  }

  /**
   * Commits the snapshot to disk.
   */
  public void commit() {
    snapshot.complete();
  }

  /**
   * Closes and deletes the snapshot.
   */
  public void rollback() {
    snapshot.close();
    snapshot.delete();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("snapshot", snapshot)
        .add("nextOffset", nextOffset)
        .toString();
  }
}
