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
 * limitations under the License.
 */
package io.atomix.protocols.raft.server.storage;

import io.atomix.protocols.raft.server.storage.snapshot.SnapshotFile;
import io.atomix.protocols.raft.server.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.server.storage.system.MetaStore;
import io.atomix.util.Assert;
import io.atomix.util.serializer.KryoNamespaces;
import io.atomix.util.serializer.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Predicate;

/**
 * Immutable log configuration and {@link Log} factory.
 * <p>
 * This class provides a factory for {@link Log} objects. {@code Storage} objects are immutable and
 * can be created only via the {@link Storage.Builder}. To create a new
 * {@code Storage.Builder}, use the static {@link #builder()} factory method:
 * <pre>
 *   {@code
 *     Storage storage = Storage.builder()
 *       .withDirectory(new File("logs"))
 *       .withStorageLevel(StorageLevel.DISK)
 *       .build();
 *   }
 * </pre>
 *
 * @see Log
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Storage {

  /**
   * Returns a new storage builder.
   *
   * @return A new storage builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = 1024 * 1024;
  private static final int DEFAULT_ENTRY_BUFFER_SIZE = 1024;
  private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;
  private static final boolean DEFAULT_RETAIN_STALE_SNAPSHOTS = false;

  private StorageLevel storageLevel = StorageLevel.DISK;
  private File directory = new File(DEFAULT_DIRECTORY);
  private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
  private int entryBufferSize = DEFAULT_ENTRY_BUFFER_SIZE;
  private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;
  private boolean retainStaleSnapshots = DEFAULT_RETAIN_STALE_SNAPSHOTS;

  public Storage() {
  }

  public Storage(StorageLevel storageLevel) {
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(String directory) {
    this(new File(Assert.notNull(directory, "directory")));
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(File directory) {
    this(directory, StorageLevel.DISK);
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(String directory, StorageLevel storageLevel) {
    this(new File(Assert.notNull(directory, "directory")), storageLevel);
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(File directory, StorageLevel storageLevel) {
    this.directory = Assert.notNull(directory, "directory");
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
  }

  /**
   * Returns the storage directory.
   * <p>
   * The storage directory is the directory to which all {@link Log}s write {@link Segment} files. Segment files
   * for multiple logs may be stored in the storage directory, and files for each log instance will be identified
   * by the {@code name} provided when the log is {@link #openLog(String) opened}.
   *
   * @return The storage directory.
   */
  public File directory() {
    return directory;
  }

  /**
   * Returns the storage level.
   * <p>
   * The storage level dictates how entries within individual log {@link Segment}s should be stored.
   *
   * @return The storage level.
   */
  public StorageLevel level() {
    return storageLevel;
  }

  /**
   * Returns the maximum log segment size.
   * <p>
   * The maximum segment size dictates the maximum size any {@link Segment} in a {@link Log} may consume
   * in bytes.
   *
   * @return The maximum segment size in bytes.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns the maximum number of entries per segment.
   * <p>
   * The maximum entries per segment dictates the maximum number of {@link io.atomix.protocols.raft.server.storage.entry.Entry entries}
   * that are allowed to be stored in any {@link Segment} in a {@link Log}.
   *
   * @return The maximum number of entries per segment.
   */
  public int maxEntriesPerSegment() {
    return maxEntriesPerSegment;
  }

  /**
   * Returns the entry buffer size.
   * <p>
   * The entry buffer size dictates the number of entries that will be held in memory for read operations
   * at the tail of the log.
   *
   * @return The entry buffer size.
   */
  public int entryBufferSize() {
    return entryBufferSize;
  }

  /**
   *
   * Returns whether to flush buffers to disk when entries are committed.
   *
   * @return Whether to flush buffers to disk when entries are committed.
   */
  public boolean flushOnCommit() {
    return flushOnCommit;
  }

  /**
   * Returns a boolean value indicating whether to retain stale snapshots on disk.
   * <p>
   * If this option is enabled, snapshots will be retained on disk even after they no longer contribute
   * to the state of the system (there's a more recent snapshot). Users may want to disable this option
   * for backup purposes.
   *
   * @return Indicates whether to retain stale snapshots on disk.
   */
  public boolean retainStaleSnapshots() {
    return retainStaleSnapshots;
  }

  /**
   * Opens a new {@link MetaStore}, recovering metadata from disk if it exists.
   * <p>
   * The meta store will be loaded using based on the configured {@link StorageLevel}. If the storage level is persistent
   * then the meta store will be loaded from disk, otherwise a new meta store will be created.
   *
   * @param name The metastore name.
   * @return The metastore.
   */
  public MetaStore openMetaStore(String name) {
    return new MetaStore(name, this, Serializer.using(KryoNamespaces.RAFT));
  }

  /**
   * Deletes a {@link MetaStore} from disk.
   * <p>
   * The meta store will be deleted by simply reading {@code meta} file names from disk and deleting metadata
   * files directly. Deleting the meta store does not involve reading any metadata files into memory.
   *
   * @param name The metastore name.
   */
  public void deleteMetaStore(String name) {
    deleteFiles(f -> f.getName().equals(String.format("%s.meta", name)) ||
      f.getName().equals(String.format("%s.conf", name)));
  }

  /**
   * Opens a new {@link SnapshotStore}, recovering snapshots from disk if they exist.
   * <p>
   * The snapshot store will be loaded using based on the configured {@link StorageLevel}. If the storage level is persistent
   * then the snapshot store will be loaded from disk, otherwise a new snapshot store will be created.
   *
   * @param name The snapshot store name.
   * @return The snapshot store.
   */
  public SnapshotStore openSnapshotStore(String name) {
    return new SnapshotStore(name, this, Serializer.using(KryoNamespaces.RAFT));
  }

  /**
   * Deletes a {@link SnapshotStore} from disk.
   * <p>
   * The snapshot store will be deleted by simply reading {@code snapshot} file names from disk and deleting snapshot
   * files directly. Deleting the snapshot store does not involve reading any snapshot files into memory.
   *
   * @param name The snapshot store name.
   */
  public void deleteSnapshotStore(String name) {
    deleteFiles(f -> SnapshotFile.isSnapshotFile(name, f));
  }

  /**
   * Opens a new {@link Log}, recovering the log from disk if it exists.
   * <p>
   * When a log is opened, the log will attempt to load {@link Segment}s from the storage {@link #directory()}
   * according to the provided log {@code name}. If segments for the given log name are present on disk, segments
   * will be loaded and indexes will be rebuilt from disk. If no segments are found, an empty log will be created.
   * <p>
   * When log files are loaded from disk, the file names are expected to be based on the provided log {@code name}.
   *
   * @param name The log name.
   * @return The opened log.
   */
  public Log openLog(String name) {
    return new Log(name, this);
  }

  /**
   * Deletes a {@link Log} from disk.
   * <p>
   * The log will be deleted by simply reading {@code log} file names from disk and deleting log files directly.
   * Deleting log files does not involve rebuilding indexes or reading any logs into memory.
   *
   * @param name The log name.
   */
  public void deleteLog(String name) {
    deleteFiles(f -> SegmentFile.isSegmentFile(name, f));
  }

  /**
   * Deletes file in the storage directory that match the given predicate.
   */
  private void deleteFiles(Predicate<File> predicate) {
    directory.mkdirs();

    // Iterate through all files in the storage directory.
    for (File file : directory.listFiles(f -> f.isFile() && predicate.test(f))) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        // Ignore the exception.
      }
    }
  }

  @Override
  public String toString() {
    return String.format("%s[directory=%s]", getClass().getSimpleName(), directory);
  }

  /**
   * Builds a {@link Storage} configuration.
   * <p>
   * The storage builder provides simplifies building more complex {@link Storage} configurations. To
   * create a storage builder, use the {@link #builder()} factory method. Set properties of the configured
   * {@code Storage} object with the various {@code with*} methods. Once the storage has been configured,
   * call {@link #build()} to build the object.
   * <pre>
   *   {@code
   *   Storage storage = Storage.builder()
   *     .withDirectory(new File("logs"))
   *     .withPersistenceLevel(PersistenceLevel.DISK)
   *     .build();
   *   }
   * </pre>
   */
  public static class Builder implements io.atomix.util.Builder<Storage> {
    private final Storage storage = new Storage();

    private Builder() {
    }

    /**
     * Sets the log storage level, returning the builder for method chaining.
     * <p>
     * The storage level indicates how individual {@link io.atomix.protocols.raft.server.storage.entry.Entry entries}
     * should be persisted in the log.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      storage.storageLevel = Assert.notNull(storageLevel, "storageLevel");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. If multiple {@link Storage} objects are located
     * on the same machine, they write logs to different directories.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      return withDirectory(new File(Assert.notNull(directory, "directory")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. If multiple {@link Storage} objects are located
     * on the same machine, they write logs to different directories.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      storage.directory = Assert.notNull(directory, "directory");
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     * <p>
     * The maximum segment size dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, once the size of the segment surpasses the configured maximum segment size, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      Assert.arg(maxSegmentSize > SegmentDescriptor.BYTES, "maxSegmentSize must be greater than " + SegmentDescriptor.BYTES);
      storage.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment, returning the builder for method chaining.
     * <p>
     * The maximum entry count dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, if the entry count in that segment meets the configured maximum entry count, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum entries per segment is {@code 1024 * 1024}.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxEntriesPerSegment} not greater than the default max entries per
     * segment
     */
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      Assert.arg(maxEntriesPerSegment > 0, "max entries per segment must be positive");
      Assert.argNot(maxEntriesPerSegment > DEFAULT_MAX_ENTRIES_PER_SEGMENT,
        "max entries per segment cannot be greater than " + DEFAULT_MAX_ENTRIES_PER_SEGMENT);
      storage.maxEntriesPerSegment = maxEntriesPerSegment;
      return this;
    }

    /**
     * Sets the entry buffer size.
     * <p>
     * The entry buffer size dictates the number of entries to hold in memory at the tail of the log. Increasing
     * the buffer size increases the number of entries that will be held in memory and thus implies greater memory
     * consumption, but server performance may be improved due to reduced disk access.
     *
     * @param entryBufferSize The entry buffer size.
     * @return The storage builder.
     * @throws IllegalArgumentException if the buffer size is not positive
     */
    public Builder withEntryBufferSize(int entryBufferSize) {
      storage.entryBufferSize = Assert.arg(entryBufferSize, entryBufferSize > 0, "entryBufferSize must be positive");
      return this;
    }

    /**
     * Enables flushing buffers to disk when entries are committed to a segment, returning the builder
     * for method chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
     * an entry is committed in a given segment.
     *
     * @return The storage builder.
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder
     * for method chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
     * an entry is committed in a given segment.
     *
     * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
     * @return The storage builder.
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      storage.flushOnCommit = flushOnCommit;
      return this;
    }

    /**
     * Enables retaining stale snapshots on disk, returning the builder for method chaining.
     * <p>
     * As the system state progresses, periodic snapshots of the state machine's state are taken.
     * Once a new snapshot of the state machine is taken, all preceding snapshots no longer contribute
     * to the state of the system and can therefore be removed from disk. By default, snapshots will not
     * be retained once a new snapshot is stored on disk. Enabling snapshot retention will ensure that
     * all snapshots will be saved, e.g. for backup purposes.
     *
     * @return The storage builder.
     */
    public Builder withRetainStaleSnapshots() {
      return withRetainStaleSnapshots(true);
    }

    /**
     * Sets whether to retain stale snapshots on disk, returning the builder for method chaining.
     * <p>
     * As the system state progresses, periodic snapshots of the state machine's state are taken.
     * Once a new snapshot of the state machine is taken, all preceding snapshots no longer contribute
     * to the state of the system and can therefore be removed from disk. By default, snapshots will not
     * be retained once a new snapshot is stored on disk. Enabling snapshot retention will ensure that
     * all snapshots will be saved, e.g. for backup purposes.
     *
     * @param retainStaleSnapshots Whether to retain stale snapshots on disk.
     * @return The storage builder.
     */
    public Builder withRetainStaleSnapshots(boolean retainStaleSnapshots) {
      storage.retainStaleSnapshots = retainStaleSnapshots;
      return this;
    }

    /**
     * Builds the {@link Storage} object.
     *
     * @return The built storage configuration.
     */
    @Override
    public Storage build() {
      return storage;
    }
  }

}
