// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft storage test.
 */
public class RaftStorageTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  @Test
  public void testDefaultConfiguration() throws Exception {
    RaftStorage storage = RaftStorage.builder().build();
    assertEquals("atomix", storage.prefix());
    assertEquals(new File(System.getProperty("user.dir")), storage.directory());
    assertEquals(1024 * 1024 * 32, storage.maxLogSegmentSize());
    assertEquals(1024 * 1024, storage.maxLogEntriesPerSegment());
    assertTrue(storage.dynamicCompaction());
    assertEquals(.2, storage.freeDiskBuffer(), .01);
    assertTrue(storage.isFlushOnCommit());
    assertFalse(storage.isRetainStaleSnapshots());
    assertTrue(storage.statistics().getFreeMemory() > 0);
  }

  @Test
  public void testCustomConfiguration() throws Exception {
    RaftStorage storage = RaftStorage.builder()
        .withPrefix("foo")
        .withDirectory(new File(PATH.toFile(), "foo"))
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(1024)
        .withDynamicCompaction(false)
        .withFreeDiskBuffer(.5)
        .withFlushOnCommit(false)
        .withRetainStaleSnapshots()
        .build();
    assertEquals("foo", storage.prefix());
    assertEquals(new File(PATH.toFile(), "foo"), storage.directory());
    assertEquals(1024 * 1024, storage.maxLogSegmentSize());
    assertEquals(1024, storage.maxLogEntriesPerSegment());
    assertFalse(storage.dynamicCompaction());
    assertEquals(.5, storage.freeDiskBuffer(), .01);
    assertFalse(storage.isFlushOnCommit());
    assertTrue(storage.isRetainStaleSnapshots());
  }

  @Test
  public void testCustomConfiguration2() throws Exception {
    RaftStorage storage = RaftStorage.builder()
        .withDirectory(PATH.toString() + "/baz")
        .withDynamicCompaction()
        .withFlushOnCommit()
        .build();
    assertEquals(new File(PATH.toFile(), "baz"), storage.directory());
    assertTrue(storage.dynamicCompaction());
    assertTrue(storage.isFlushOnCommit());
  }

  @Test
  public void testStorageLock() throws Exception {
    RaftStorage storage1 = RaftStorage.builder()
        .withDirectory(PATH.toFile())
        .withPrefix("test")
        .build();

    assertTrue(storage1.lock("a"));

    RaftStorage storage2 = RaftStorage.builder()
        .withDirectory(PATH.toFile())
        .withPrefix("test")
        .build();

    assertFalse(storage2.lock("b"));

    RaftStorage storage3 = RaftStorage.builder()
        .withDirectory(PATH.toFile())
        .withPrefix("test")
        .build();

    assertTrue(storage3.lock("a"));
  }

  @Before
  @After
  public void cleanupStorage() throws IOException {
    if (Files.exists(PATH)) {
      Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
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
  }
}
