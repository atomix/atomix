/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Snapshot store test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileSnapshotStoreTest extends AbstractSnapshotStoreTest {
  private String testId;

  /**
   * Returns a new snapshot store.
   */
  protected SnapshotStore createSnapshotStore() {
    RaftStorage storage = RaftStorage.builder()
        .withPrefix("test")
        .withDirectory(new File(String.format("target/test-logs/%s", testId)))
        .withStorageLevel(StorageLevel.DISK)
        .build();
    return new SnapshotStore(storage);
  }

  /**
   * Tests storing and loading snapshots.
   */
  @Test
  public void testStoreLoadSnapshot() {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(2, new WallClockTimestamp());
    try (SnapshotWriter writer = snapshot.openWriter()) {
      writer.writeLong(10);
    }
    snapshot.complete();
    assertNotNull(store.getSnapshot(2));
    store.close();

    store = createSnapshotStore();
    assertNotNull(store.getSnapshot(2));
    assertEquals(2, store.getSnapshot(2).index());

    try (SnapshotReader reader = snapshot.openReader()) {
      assertEquals(10, reader.readLong());
    }
  }

  /**
   * Tests persisting and loading snapshots.
   */
  @Test
  public void testPersistLoadSnapshot() {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(2, new WallClockTimestamp());
    try (SnapshotWriter writer = snapshot.openWriter()) {
      writer.writeLong(10);
    }

    assertNull(store.getSnapshot(2));
    assertTempSnapshotCount(store, 1);
    snapshot.complete();
    assertTempSnapshotCount(store, 0);
    assertNotNull(store.getSnapshot(2));

    try (SnapshotReader reader = snapshot.openReader()) {
      assertEquals(10, reader.readLong());
    }

    store.close();

    store = createSnapshotStore();
    assertNotNull(store.getSnapshot(2));
    assertEquals(2, store.getSnapshot(2).index());

    snapshot = store.getSnapshot(2);
    try (SnapshotReader reader = snapshot.openReader()) {
      assertEquals(10, reader.readLong());
    }
  }

  /**
   * Tests writing multiple times to a snapshot designed to mimic chunked snapshots from leaders.
   */
  @Test
  public void testStreamSnapshot() {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(1, new WallClockTimestamp());
    for (long i = 1; i <= 10; i++) {
      try (SnapshotWriter writer = snapshot.openWriter()) {
        writer.writeLong(i);
      }
    }
    snapshot.complete();

    snapshot = store.getSnapshot(1);
    try (SnapshotReader reader = snapshot.openReader()) {
      for (long i = 1; i <= 10; i++) {
        assertEquals(i, reader.readLong());
      }
    }
  }

  /**
   * Tests case where two {@link FileSnapshot} instances are trying to write the same snapshot
   */
  @Test
  public void testConcurrentSnapshotWriters() {
    SnapshotStore store = createSnapshotStore();
    final WallClockTimestamp timestamp = new WallClockTimestamp();
    Snapshot first = store.newSnapshot(1, timestamp);
    Snapshot second = store.newSnapshot(1, timestamp);

    try (SnapshotWriter firstWriter = first.openWriter()) {
      firstWriter.writeLong(1);
    }

    try (SnapshotWriter secondWriter = second.openWriter()) {
      secondWriter.writeLong(1);
    }

    first.complete();
    second.complete();

    Snapshot completed = store.getSnapshot(first.index());
    assertNotNull(completed);
    long result = 0;
    try (SnapshotReader reader = completed.openReader()) {
      while (reader.hasRemaining()) {
        result += reader.readLong();
      }
    }

    assertEquals(result, 1);
  }

  @Test
  public void testTemporarySnapshotCleanedUpOnClose() {
    final SnapshotStore store = createSnapshotStore();
    final Snapshot snapshot = store.newSnapshot(1, new WallClockTimestamp());

    assertTempSnapshotCount(store, 1);
    snapshot.close();
    assertTempSnapshotCount(store, 0);
  }

  @Before
  @After
  public void cleanupStorage() throws IOException {
    Path directory = Paths.get("target/test-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
    testId = UUID.randomUUID().toString();
  }

  private void assertTempSnapshotCount(SnapshotStore store, int expected) {
    final File[] tempSnapshots = store.storage.directory().listFiles(f -> f.getName().endsWith(".tmp"));
    assertEquals(expected, tempSnapshots != null ? tempSnapshots.length : 0);
  }
}
