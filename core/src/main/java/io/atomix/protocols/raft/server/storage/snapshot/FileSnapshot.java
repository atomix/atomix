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
 * limitations under the License
 */
package io.atomix.protocols.raft.server.storage.snapshot;

import io.atomix.util.Assert;
import io.atomix.util.buffer.Buffer;
import io.atomix.util.buffer.FileBuffer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * File-based snapshot backed by a {@link FileBuffer}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class FileSnapshot extends Snapshot {
  private final SnapshotFile file;
  private final SnapshotStore store;

  FileSnapshot(SnapshotFile file, SnapshotStore store) {
    super(store);
    this.file = Assert.notNull(file, "file");
    this.store = Assert.notNull(store, "store");
  }

  @Override
  public long id() {
    return file.id();
  }

  @Override
  public long index() {
    return file.index();
  }

  @Override
  public long timestamp() {
    return file.timestamp();
  }

  @Override
  public synchronized SnapshotWriter writer() {
    checkWriter();
    SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
      .withIndex(file.index())
      .withTimestamp(file.timestamp())
      .build();

    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES);
    descriptor.copyTo(buffer);

    int length = buffer.position(SnapshotDescriptor.BYTES).readInt();
    return openWriter(new SnapshotWriter(buffer.skip(length).mark(), this, store.serializer()), descriptor);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    int length = (int) (writer.buffer.position() - (SnapshotDescriptor.BYTES + Integer.BYTES));
    writer.buffer.writeInt(SnapshotDescriptor.BYTES, length).flush();
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader reader() {
    Assert.state(file.file().exists(), "missing snapshot file: %s", file.file());
    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES);
    SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer);
    int length = buffer.position(SnapshotDescriptor.BYTES).readInt();
    return openReader(new SnapshotReader(buffer.mark().limit(SnapshotDescriptor.BYTES + Integer.BYTES + length), this, store.serializer()), descriptor);
  }

  @Override
  public Snapshot complete() {
    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES);
    try (SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer)) {
      Assert.stateNot(descriptor.locked(), "cannot complete locked snapshot descriptor");
      descriptor.lock();
    }
    return super.complete();
  }

  /**
   * Deletes the snapshot file.
   */
  @Override
  public void delete() {
    Path path = file.file().toPath();
    if (Files.exists(path)) {
      try {
        Files.delete(file.file().toPath());
      } catch (IOException e) {
      }
    }
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d]", getClass().getSimpleName(), index());
  }

}
