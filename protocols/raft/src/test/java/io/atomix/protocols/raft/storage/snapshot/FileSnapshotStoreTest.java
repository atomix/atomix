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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * File snapshot store test.
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

    Snapshot snapshot = store.newTemporarySnapshot(2, new WallClockTimestamp());
    try (SnapshotWriter writer = snapshot.openWriter()) {
      writer.writeLong(10);
    }

    snapshot = snapshot.persist();

    assertTrue(snapshot.isPersisted());

    assertNull(store.getSnapshot(2));

    snapshot.complete();
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
  public void testStreamSnapshot() throws Exception {
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

}
