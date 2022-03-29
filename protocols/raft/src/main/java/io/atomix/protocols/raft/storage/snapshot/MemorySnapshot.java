// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.storage.buffer.HeapBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In-memory snapshot backed by a {@link HeapBuffer}.
 */
final class MemorySnapshot extends Snapshot {
  private final HeapBuffer buffer;
  private final SnapshotDescriptor descriptor;

  MemorySnapshot(HeapBuffer buffer, SnapshotDescriptor descriptor, SnapshotStore store) {
    super(descriptor, store);
    buffer.mark();
    this.buffer = checkNotNull(buffer, "buffer cannot be null");
    this.buffer.position(SnapshotDescriptor.BYTES).mark();
    this.descriptor = checkNotNull(descriptor, "descriptor cannot be null");
  }

  @Override
  public SnapshotWriter openWriter() {
    checkWriter();
    return new SnapshotWriter(buffer.reset().slice(), this);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    buffer.skip(writer.buffer.position()).mark();
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader openReader() {
    return openReader(new SnapshotReader(buffer.reset().slice(), this), descriptor);
  }

  @Override
  public Snapshot complete() {
    buffer.flip().skip(SnapshotDescriptor.BYTES).mark();
    descriptor.lock();
    return super.complete();
  }

  @Override
  public void close() {
    buffer.close();
  }
}
