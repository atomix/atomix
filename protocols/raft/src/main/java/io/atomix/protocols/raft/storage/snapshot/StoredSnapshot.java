/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.snapshot;

/**
 * Snapshot that's stored in the SnapshotStore.
 */
public abstract class StoredSnapshot extends Snapshot {
  protected final SnapshotStore store;

  public StoredSnapshot(SnapshotDescriptor descriptor, SnapshotStore store) {
    super(descriptor);
    this.store = store;
  }

  @Override
  public Snapshot complete() {
    store.completeSnapshot(this);
    return this;
  }
}
