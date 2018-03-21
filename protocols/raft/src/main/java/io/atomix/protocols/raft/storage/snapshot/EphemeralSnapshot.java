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

import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Special snapshot that is not stored in the SnapshotStore.
 */
public class EphemeralSnapshot extends Snapshot {
  private final Buffer buffer;

  public EphemeralSnapshot(SnapshotDescriptor descriptor) {
    this(HeapBuffer.allocate(), descriptor);
  }

  public EphemeralSnapshot(Buffer buffer, SnapshotDescriptor descriptor) {
    super(descriptor);
    this.buffer = checkNotNull(buffer);
  }

  @Override
  public SnapshotWriter openWriter() {
    return new SnapshotWriter(buffer, this);
  }

  @Override
  public SnapshotReader openReader() {
    return new SnapshotReader(buffer, this);
  }

  @Override
  public boolean isPersisted() {
    return false;
  }

  @Override
  public Snapshot complete() {
    buffer.flip();
    return this;
  }
}
