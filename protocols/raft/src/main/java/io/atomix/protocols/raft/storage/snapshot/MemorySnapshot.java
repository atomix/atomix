/*
 * Copyright 2015-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.serializer.Serializer;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.buffer.HeapBuffer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In-memory snapshot backed by a {@link HeapBuffer}.
 */
final class MemorySnapshot extends Snapshot {
  private final HeapBuffer buffer;
  private final SnapshotDescriptor descriptor;
  private final SnapshotStore store;

  MemorySnapshot(HeapBuffer buffer, SnapshotDescriptor descriptor, SnapshotStore store) {
    super(store);
    buffer.mark();
    this.buffer = checkNotNull(buffer, "buffer cannot be null");
    this.buffer.position(SnapshotDescriptor.BYTES).mark();
    this.descriptor = checkNotNull(descriptor, "descriptor cannot be null");
    this.store = checkNotNull(store, "store cannot be null");
  }

  @Override
  public SnapshotId getSnapshotId() {
    return SnapshotId.of(descriptor.getId());
  }

  @Override
  public long getIndex() {
    return descriptor.getIndex();
  }

  @Override
  public long getTimestamp() {
    return descriptor.getTimestamp();
  }

  @Override
  public SnapshotWriter openWriter(Serializer serializer) {
    checkWriter();
    return new SnapshotWriter(buffer.reset().slice(), this, serializer);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    buffer.skip(writer.buffer.position()).mark();
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader openReader(Serializer serializer) {
    return openReader(new SnapshotReader(buffer.reset().slice(), this, serializer), descriptor);
  }

  @Override
  public Snapshot persist() {
    if (store.storage.getStorageLevel() != StorageLevel.MEMORY) {
      try (Snapshot newSnapshot = store.newSnapshot(getSnapshotId(), getIndex())) {
        try (SnapshotWriter newSnapshotWriter = newSnapshot.openWriter(store.storage.getSerializer())) {
          buffer.flip();
          newSnapshotWriter.write(buffer.array(), 0, buffer.remaining());
        }
        return newSnapshot;
      }
    }
    return this;
  }

  @Override
  public Snapshot complete() {
    descriptor.lock();
    return super.complete();
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", getIndex())
        .toString();
  }

}
