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

import io.atomix.protocols.raft.RaftServer;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.storage.buffer.Bytes;

import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Writes bytes to a state machine {@link Snapshot}.
 * <p>
 * This class provides the primary interface for writing snapshot buffers to disk or memory.
 * Snapshot bytes are written to an underlying {@link Buffer} which is backed by either memory
 * or disk based on the configured {@link StorageLevel}.
 * <p>
 * In addition to standard {@link BufferOutput} methods, snapshot readers support writing serializable objects
 * to the snapshot via the {@link #writeObject(Object, java.util.function.Function)} method. Serializable types must be registered on the
 * {@link RaftServer} serializer to be supported in snapshots.
 */
public class SnapshotWriter implements BufferOutput<SnapshotWriter> {
  final Buffer buffer;
  private final Snapshot snapshot;

  public SnapshotWriter(Buffer buffer, Snapshot snapshot) {
    this.buffer = checkNotNull(buffer, "buffer cannot be null");
    this.snapshot = checkNotNull(snapshot, "snapshot cannot be null");
  }

  /**
   * Returns the snapshot associated with the writer.
   *
   * @return The snapshot associated with the writer
   */
  public Snapshot snapshot() {
    return snapshot;
  }

  /**
   * Returns the snapshot buffer.
   *
   * @return the snapshot buffer
   */
  public Buffer buffer() {
    return buffer;
  }

  @Override
  public SnapshotWriter write(Bytes bytes) {
    buffer.write(bytes);
    return this;
  }

  @Override
  public SnapshotWriter write(byte[] bytes) {
    buffer.write(bytes);
    return this;
  }

  @Override
  public SnapshotWriter write(Bytes bytes, int offset, int length) {
    buffer.write(bytes, offset, length);
    return this;
  }

  @Override
  public SnapshotWriter write(byte[] bytes, int offset, int length) {
    buffer.write(bytes, offset, length);
    return this;
  }

  @Override
  public SnapshotWriter write(Buffer buffer) {
    this.buffer.write(buffer);
    return this;
  }

  @Override
  public SnapshotWriter writeByte(int b) {
    buffer.writeByte(b);
    return this;
  }

  @Override
  public SnapshotWriter writeUnsignedByte(int b) {
    buffer.writeUnsignedByte(b);
    return this;
  }

  @Override
  public SnapshotWriter writeChar(char c) {
    buffer.writeChar(c);
    return this;
  }

  @Override
  public SnapshotWriter writeShort(short s) {
    buffer.writeShort(s);
    return this;
  }

  @Override
  public SnapshotWriter writeUnsignedShort(int s) {
    buffer.writeUnsignedShort(s);
    return this;
  }

  @Override
  public SnapshotWriter writeMedium(int m) {
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public SnapshotWriter writeUnsignedMedium(int m) {
    buffer.writeUnsignedMedium(m);
    return this;
  }

  @Override
  public SnapshotWriter writeInt(int i) {
    buffer.writeInt(i);
    return this;
  }

  @Override
  public SnapshotWriter writeUnsignedInt(long i) {
    buffer.writeUnsignedInt(i);
    return this;
  }

  @Override
  public SnapshotWriter writeLong(long l) {
    buffer.writeLong(l);
    return this;
  }

  @Override
  public SnapshotWriter writeFloat(float f) {
    buffer.writeFloat(f);
    return this;
  }

  @Override
  public SnapshotWriter writeDouble(double d) {
    buffer.writeDouble(d);
    return this;
  }

  @Override
  public SnapshotWriter writeBoolean(boolean b) {
    buffer.writeBoolean(b);
    return this;
  }

  @Override
  public SnapshotWriter writeString(String s) {
    buffer.writeString(s);
    return this;
  }

  @Override
  public SnapshotWriter writeString(String s, Charset charset) {
    buffer.writeString(s, charset);
    return this;
  }

  @Override
  public SnapshotWriter writeUTF8(String s) {
    buffer.writeUTF8(s);
    return this;
  }

  @Override
  public SnapshotWriter flush() {
    buffer.flush();
    return this;
  }

  @Override
  public void close() {
    buffer.flush();
    snapshot.closeWriter(this);
    buffer.close();
  }

}
