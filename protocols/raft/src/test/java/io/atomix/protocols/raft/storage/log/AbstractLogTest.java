/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.log;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractLogTest {
  protected static final int MAX_ENTRIES_PER_SEGMENT = 10;
  protected static final int MAX_SEGMENT_SIZE = 1024 * 8;
  private static final Path PATH = Paths.get("target/test-logs/");

  private static final Serializer serializer = Serializer.using(KryoNamespace.builder()
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(TestEntry.class)
      .register(ArrayList.class)
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(NodeId.class)
      .register(RaftMember.Type.class)
      .register(ReadConsistency.class)
      .register(Instant.class)
      .register(byte[].class)
      .build());

  protected abstract StorageLevel storageLevel();

  protected RaftLog createLog() {
    return RaftLog.builder()
        .withName("test")
        .withDirectory(PATH.toFile())
        .withSerializer(serializer)
        .withStorageLevel(storageLevel())
        .withMaxEntriesPerSegment(MAX_ENTRIES_PER_SEGMENT)
        .withMaxSegmentSize(MAX_SEGMENT_SIZE)
        .build();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLogWriteRead() throws Exception {
    RaftLog log = createLog();
    RaftLogWriter writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.ALL);

    // Append a couple entries.
    Indexed<RaftLogEntry> indexed;
    assertEquals(writer.getNextIndex(), 1);
    indexed = writer.append(new OpenSessionEntry(1, System.currentTimeMillis(), "client", "test1", "test", ReadConsistency.LINEARIZABLE, 100, 1000));
    assertEquals(indexed.index(), 1);

    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new CloseSessionEntry(1, System.currentTimeMillis(), 1, false), 0));
    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);
    assertFalse(reader.hasNext());

    // Test reading the register entry.
    Indexed<OpenSessionEntry> openSession;
    reader.reset();
    openSession = (Indexed) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.entry().term(), 1);
    assertEquals(openSession.entry().serviceName(), "test1");
    assertEquals(openSession.entry().serviceType(), "test");
    assertEquals(openSession.entry().maxTimeout(), 1000);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);

    // Test reading the unregister entry.
    Indexed<CloseSessionEntry> closeSession;
    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = (Indexed) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.entry().term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Test opening a new reader and reading from the log.
    reader = log.openReader(1, RaftLogReader.Mode.ALL);
    assertTrue(reader.hasNext());
    openSession = (Indexed) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.entry().term(), 1);
    assertEquals(openSession.entry().serviceName(), "test1");
    assertEquals(openSession.entry().serviceType(), "test");
    assertEquals(openSession.entry().maxTimeout(), 1000);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = (Indexed) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.entry().term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Reset the reader.
    reader.reset();

    // Test opening a new reader and reading from the log.
    reader = log.openReader(1, RaftLogReader.Mode.ALL);
    assertTrue(reader.hasNext());
    openSession = (Indexed) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.entry().term(), 1);
    assertEquals(openSession.entry().serviceName(), "test1");
    assertEquals(openSession.entry().serviceType(), "test");
    assertEquals(openSession.entry().maxTimeout(), 1000);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = (Indexed) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.entry().term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Truncate the log and write a different entry.
    writer.truncate(1);
    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new CloseSessionEntry(2, System.currentTimeMillis(), 1, false), 0));
    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);
    assertEquals(indexed.entry().term(), 2);

    // Reset the reader to a specific index and read the last entry again.
    reader.reset(2);

    assertNotNull(reader.getCurrentEntry());
    assertEquals(1, reader.getCurrentIndex());
    assertEquals(1, reader.getCurrentEntry().index());
    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = (Indexed) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.entry().term(), 2);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());
  }

  @Test
  public void testResetTruncateZero() throws Exception {
    RaftLog log = createLog();
    RaftLogWriter writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.ALL);

    assertEquals(0, writer.getLastIndex());
    writer.reset(1);
    assertEquals(0, writer.getLastIndex());
    writer.append(new InitializeEntry(1, System.currentTimeMillis()));
    assertEquals(1, writer.getLastIndex());
    assertEquals(1, writer.getLastEntry().index());

    assertTrue(reader.hasNext());
    assertEquals(1, reader.next().index());

    writer.truncate(0);
    assertEquals(0, writer.getLastIndex());
    assertNull(writer.getLastEntry());
    writer.append(new InitializeEntry(1, System.currentTimeMillis()));
    assertEquals(1, writer.getLastIndex());
    assertEquals(1, writer.getLastEntry().index());

    assertTrue(reader.hasNext());
    assertEquals(1, reader.next().index());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteReadEntries() throws Exception {
    RaftLog log = createLog();
    RaftLogWriter writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.ALL);

    for (int i = 1; i <= MAX_ENTRIES_PER_SEGMENT * 5; i++) {
      writer.append(new TestEntry(1, 32));
      assertTrue(reader.hasNext());
      Indexed<TestEntry> entry;
      entry = (Indexed) reader.next();
      assertEquals(i, entry.index());
      assertEquals(1, entry.entry().term());
      assertEquals(32, entry.entry().bytes().length);
      reader.reset(i);
      entry = (Indexed) reader.next();
      assertEquals(i, entry.index());
      assertEquals(1, entry.entry().term());
      assertEquals(32, entry.entry().bytes().length);

      if (i > 6) {
        reader.reset(i - 5);
        assertNotNull(reader.getCurrentEntry());
        assertEquals(i - 6, reader.getCurrentIndex());
        assertEquals(i - 6, reader.getCurrentEntry().index());
        assertEquals(i - 5, reader.getNextIndex());
        reader.reset(i + 1);
      }

      writer.truncate(i - 1);
      writer.append(new TestEntry(1, 32));

      assertTrue(reader.hasNext());
      reader.reset(i);
      assertTrue(reader.hasNext());
      entry = (Indexed) reader.next();
      assertEquals(i, entry.index());
      assertEquals(1, entry.entry().term());
      assertEquals(32, entry.entry().bytes().length);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteReadCommittedEntries() throws Exception {
    RaftLog log = createLog();
    RaftLogWriter writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.COMMITS);

    for (int i = 1; i <= MAX_ENTRIES_PER_SEGMENT * 5; i++) {
      writer.append(new TestEntry(1, 32));
      assertFalse(reader.hasNext());
      writer.commit(i);
      assertTrue(reader.hasNext());
      Indexed<TestEntry> entry;
      entry = (Indexed) reader.next();
      assertEquals(i, entry.index());
      assertEquals(1, entry.entry().term());
      assertEquals(32, entry.entry().bytes().length);
      reader.reset(i);
      entry = (Indexed) reader.next();
      assertEquals(i, entry.index());
      assertEquals(1, entry.entry().term());
      assertEquals(32, entry.entry().bytes().length);
    }
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