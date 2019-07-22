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
 * limitations under the License
 */
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.FileBuffer;
import io.atomix.utils.AtomixIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * File-based snapshot backed by a {@link FileBuffer}.
 */
final class FileSnapshot extends Snapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSnapshot.class);
  private final SnapshotFile file;

  FileSnapshot(SnapshotFile file, SnapshotDescriptor descriptor, SnapshotStore store) {
    super(descriptor, store);
    this.file = checkNotNull(file, "file cannot be null");
  }

  @Override
  public synchronized SnapshotWriter openWriter() {
    checkWriter();
    checkState(!file.file().exists(), "cannot write to completed snapshot");
    checkNotNull(file.temporaryFile(), "missing temporary snapshot file for writing");
    Buffer buffer = FileBuffer.allocate(file.temporaryFile(), SnapshotDescriptor.BYTES);
    descriptor.copyTo(buffer);

    int length = buffer.position(SnapshotDescriptor.BYTES).readInt();
    return openWriter(new SnapshotWriter(buffer.skip(length).mark(), this), descriptor);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    int length = writer.buffer.position() - (SnapshotDescriptor.BYTES + Integer.BYTES);
    writer.buffer.writeInt(SnapshotDescriptor.BYTES, length).flush();
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader openReader() {
    checkState(file.file().exists(), "missing snapshot file: %s", file.file());
    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES);
    SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer);
    int length = buffer.position(SnapshotDescriptor.BYTES).readInt();
    return openReader(new SnapshotReader(buffer.mark().limit(SnapshotDescriptor.BYTES + Integer.BYTES + length), this), descriptor);
  }

  @Override
  public Snapshot complete() {
    checkNotNull(file.temporaryFile(), "no temporary snapshot file to read from");

    Buffer buffer = FileBuffer.allocate(file.temporaryFile(), SnapshotDescriptor.BYTES);
    try (SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer)) {
      descriptor.lock();
    }

    try {
      persistTemporaryFile();
    } catch (IOException e) {
      throw new AtomixIOException(e);
    }

    file.clearTemporaryFile();
    return super.complete();
  }

  /**
   * Deletes the temporary file
   */
  @Override
  public void close() {
    super.close();

    if (file.temporaryFile() != null) {
      deleteFileSilently(file.temporaryFile().toPath());
      file.clearTemporaryFile();
    }
  }

  /**
   * Deletes the snapshot file.
   */
  @Override
  public void delete() {
    LOGGER.debug("Deleting {}", this);
    Path path = file.file().toPath();

    if (Files.exists(path)) {
      deleteFileSilently(path);
    }
  }

  private void deleteFileSilently(Path path) {
    try {
      Files.delete(path);
    } catch (IOException e) {
      LOGGER.debug("Failed to delete snapshot file {}", path, e);
    }
  }

  private void persistTemporaryFile() throws IOException {
    final Path temporaryPath = file.temporaryFile().toPath();
    final Path persistedPath = file.file().toPath();

    try {
      Files.move(temporaryPath, persistedPath, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      try {
        Files.move(temporaryPath, persistedPath);
      } catch (FileAlreadyExistsException ignored) {
        LOGGER.debug("Snapshot {} was already previously completed", this);
      }
    }
  }
}
