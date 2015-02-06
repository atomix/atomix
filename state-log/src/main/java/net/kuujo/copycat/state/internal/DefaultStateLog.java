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
package net.kuujo.copycat.state.internal;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.resource.internal.ResourceContext;
import net.kuujo.copycat.state.StateLog;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.internal.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Default state log partition implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("rawtypes")
public class DefaultStateLog<T> extends AbstractResource<StateLog<T>> implements StateLog<T> {
  private static final int SNAPSHOT_ENTRY = 0;
  private static final int COMMAND_ENTRY = 1;
  private static final int SNAPSHOT_CHUNK_SIZE = 1024 * 1024;
  private static final int SNAPSHOT_INFO = 0;
  private static final int SNAPSHOT_CHUNK = 1;
  private final Map<Integer, OperationInfo> operations = new ConcurrentHashMap<>(128);
  private final Consistency defaultConsistency;
  private final SnapshottableLogManager log;
  private Supplier snapshotter;
  private Consumer installer;
  private SnapshotInfo snapshotInfo;
  private List<ByteBuffer> snapshotChunks;

  public DefaultStateLog(ResourceContext context) {
    super(context);
    this.log = (SnapshottableLogManager) context.log();
    defaultConsistency = context.config()
      .<StateLogConfig>getResourceConfig()
      .getDefaultConsistency();
    context.consumer(this::consume);
  }

  @Override
  public <U extends T, V> StateLog<T> registerCommand(String name, Function<U, V> command) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo<>(command, false));
    return this;
  }

  @Override
  public StateLog<T> unregisterCommand(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    Assert.isNotNull(name, "name");
    Assert.isNotNull(query, "query");
    operations.put(name.hashCode(), new OperationInfo<>(query, true, defaultConsistency));
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query, Consistency consistency) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    Assert.isNotNull(name, "name");
    Assert.isNotNull(query, "query");
    operations.put(name.hashCode(), new OperationInfo<>(query, true, consistency == null || consistency == Consistency.DEFAULT ? defaultConsistency : consistency));
    return this;
  }

  @Override
  public StateLog<T> unregisterQuery(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public StateLog<T> unregister(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public <V> StateLog<T> snapshotWith(Supplier<V> snapshotter) {
    Assert.state(isClosed(), "Cannot modify state log once opened");
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public <V> StateLog<T> installWith(Consumer<V> installer) {
    Assert.state(isClosed(), "Cannot modify state log once opened");
    this.installer = installer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> CompletableFuture<U> submit(String command, T entry) {
    Assert.state(isOpen(), "State log not open");
    OperationInfo<T, U> operationInfo = operations.get(command.hashCode());
    if (operationInfo == null) {
      return Futures.exceptionalFutureAsync(new CopycatException(String.format("Invalid state log command %s", command)), executor);
    }

    // If this is a read-only command, check if the command is consistent. For consistent operations,
    // queries are forwarded to the current cluster leader for evaluation. Otherwise, it's safe to
    // read stale data from the local node.
    ByteBuffer buffer = serializer.writeObject(entry);
    ByteBuffer commandEntry = ByteBuffer.allocate(8 + buffer.capacity());
    commandEntry.putInt(COMMAND_ENTRY); // Entry type
    commandEntry.putInt(command.hashCode());
    commandEntry.put(buffer);
    commandEntry.rewind();
    if (operationInfo.readOnly) {
      return context.query(commandEntry, operationInfo.consistency).thenApplyAsync(serializer::readObject, executor);
    } else {
      return context.commit(commandEntry).thenApplyAsync(serializer::readObject, executor);
    }
  }

  @Override
  public synchronized CompletableFuture<StateLog<T>> open() {
    return runStartupTasks()
      .thenComposeAsync(v -> context.open(), executor)
      .thenApply(v -> this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return context.close()
      .thenComposeAsync(v -> runShutdownTasks(), executor);
  }

  /**
   * Consumes a log entry.
   *
   * @param index The entry index.
   * @param entry The log entry.
   * @return The entry output.
   */
  @SuppressWarnings({"unchecked"})
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    int entryType = entry.getInt();
    switch (entryType) {
      case SNAPSHOT_ENTRY: // Snapshot entry
        installSnapshot(entry.slice());
        return ByteBuffer.allocate(0);
      case COMMAND_ENTRY: // Command entry
        int commandCode = entry.getInt();
        OperationInfo operationInfo = operations.get(commandCode);
        if (operationInfo != null) {
          return serializer.writeObject(operationInfo.execute(index, serializer.readObject(entry.slice())));
        }
        throw new IllegalStateException("Invalid state log operation");
      default:
        throw new IllegalArgumentException("Invalid entry type");
    }
  }

  /**
   * Checks whether to take a snapshot.
   */
  private void checkSnapshot(long index) {
    // If the given index is the last index of a lot segment and the segment is not the last segment in the log
    // then the index is considered snapshottable.
    if (log.isSnapshottable(index)) {
      takeSnapshot(index);
    }
  }

  /**
   * Takes a snapshot and compacts the log.
   */
  private void takeSnapshot(long index) {
    Object snapshot = snapshotter != null ? snapshotter.get() : null;
    ByteBuffer snapshotBuffer = serializer.writeObject(snapshot);
    snapshotBuffer.flip();

    // Create a unique snapshot ID and calculate the number of chunks for the snapshot.
    byte[] snapshotId = UUID.randomUUID().toString().getBytes();
    int numChunks = (int) Math.ceil(snapshotBuffer.limit() / (double) SNAPSHOT_CHUNK_SIZE);
    List<ByteBuffer> chunks = new ArrayList<>(numChunks);

    // The first entry in the snapshot is snapshot metadata.
    ByteBuffer info = ByteBuffer.allocate(20 + snapshotId.length);
    info.putInt(SNAPSHOT_ENTRY);
    info.putInt(SNAPSHOT_INFO);
    info.putInt(snapshotId.length);
    info.put(snapshotId);
    info.putInt(snapshotBuffer.limit());
    info.putInt(numChunks);

    // Now we append a list of snapshot chunks. This ensures that snapshots can be easily replicated in chunks.
    int i = 0;
    int position = 0;
    while (position < snapshotBuffer.limit()) {
      byte[] bytes = new byte[Math.min(snapshotBuffer.limit() - position, SNAPSHOT_CHUNK_SIZE)];
      snapshotBuffer.get(bytes);
      ByteBuffer chunk = ByteBuffer.allocate(16 + bytes.length);
      chunk.putInt(SNAPSHOT_ENTRY); // Indicates the entry is a snapshot entry.
      chunk.putInt(SNAPSHOT_CHUNK);
      chunk.putInt(i++); // The position of the chunk in the snapshot.
      chunk.putInt(bytes.length); // The snapshot chunk length.
      chunk.put(bytes); // The snapshot chunk bytes.
      chunk.flip();
      chunks.add(chunk);
      position += bytes.length;
    }

    try {
      log.appendSnapshot(index, chunks);
    } catch (IOException e) {
      throw new CopycatException("Failed to compact state log", e);
    }
  }

  /**
   * Installs a snapshot.
   *
   * This method operates on the assumption that snapshots will always be replicated with an initial metadata entry.
   * The metadata entry specifies a set of chunks that complete the entire snapshot.
   */
  @SuppressWarnings("unchecked")
  private void installSnapshot(ByteBuffer snapshotChunk) {
    // Get the snapshot entry type.
    int type = snapshotChunk.getInt();
    if (type == SNAPSHOT_INFO) {
      // The snapshot info defines the snapshot ID and number of chunks. When a snapshot info entry is processed,
      // reset the current snapshot chunks and store the snapshot info. The next entries to be applied should be the
      // snapshot chunks themselves.
      int idLength = snapshotChunk.getInt();
      byte[] idBytes = new byte[idLength];
      snapshotChunk.get(idBytes);
      String id = new String(idBytes);
      int size = snapshotChunk.getInt();
      int numChunks = snapshotChunk.getInt();
      if (snapshotInfo == null || !snapshotInfo.id.equals(id)) {
        snapshotInfo = new SnapshotInfo(id, size, numChunks);
        snapshotChunks = new ArrayList<>(numChunks);
      }
    } else if (type == SNAPSHOT_CHUNK && snapshotInfo != null) {
      // When a chunk is received, use the chunk's position in the snapshot to ensure consistency. Extract the chunk
      // bytes and only append the the chunk if it matches the expected position in the local chunks list.
      int index = snapshotChunk.getInt();
      int chunkLength = snapshotChunk.getInt();
      byte[] chunkBytes = new byte[chunkLength];
      snapshotChunk.get(chunkBytes);
      if (snapshotChunks.size() == index) {
        snapshotChunks.add(ByteBuffer.wrap(chunkBytes));

        // Once the number of chunks has grown to the complete expected chunk count, combine and install the snapshot.
        if (snapshotChunks.size() == snapshotInfo.chunks) {
          if (installer != null) {
            // Calculate the total aggregated size of the snapshot.
            int size = 0;
            for (ByteBuffer chunk : snapshotChunks) {
              size += chunk.limit();
            }

            // Make sure the combined snapshot size is equal to the expected snapshot size.
            Assert.state(size == snapshotInfo.size, "Received inconsistent snapshot");

            // Create a complete view of the snapshot by appending all chunks to each other.
            ByteBuffer completeSnapshot = ByteBuffer.allocate(size);
            for (ByteBuffer chunk : snapshotChunks) {
              completeSnapshot.put(chunk);
            }

            // Once a view of the snapshot has been created, deserialize and install the snapshot.
            Object value = serializer.readObject(completeSnapshot);
            installer.accept(value);
          }
          snapshotInfo = null;
          snapshotChunks = null;
        }
      }
    }
  }

  /**
   * Current snapshot info.
   */
  private static class SnapshotInfo {
    private final String id;
    private final int size;
    private final int chunks;

    private SnapshotInfo(String id, int size, int chunks) {
      this.id = id;
      this.size = size;
      this.chunks = chunks;
    }
  }

  /**
   * Snapshot chunk.
   */
  private static class SnapshotChunk {
    private final int index;
    private final ByteBuffer chunk;

    private SnapshotChunk(int index, ByteBuffer chunk) {
      this.index = index;
      this.chunk = chunk;
    }
  }

  @Override
  public String toString() {
    return String.format("%s[name=%s]", getClass().getSimpleName(), context.name());
  }

  /**
   * State command info.
   */
  private class OperationInfo<TT, U> {
    private final Function<TT, U> function;
    private final boolean readOnly;
    private final Consistency consistency;

    private OperationInfo(Function<TT, U> function, boolean readOnly) {
      this(function, readOnly, Consistency.DEFAULT);
    }

    private OperationInfo(Function<TT, U> function, boolean readOnly, Consistency consistency) {
      this.function = function;
      this.readOnly = readOnly;
      this.consistency = consistency;
    }

    private U execute(Long index, TT entry) {
      if (index != null)
        checkSnapshot(index);
      return function.apply(entry);
    }
  }

}
