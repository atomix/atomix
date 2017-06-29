/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
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
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class LogTest {
  private static final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(ArrayList.class)
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .build());

  private RaftLog createLog() {
    return RaftLog.builder()
        .withName("test")
        .withSerializer(serializer)
        .withStorageLevel(StorageLevel.MEMORY)
        .build();
  }

  public void testLogWriteRead() throws Exception {
    RaftLog log = createLog();
    RaftLogWriter writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.ALL);

    // Append a couple entries.
    Indexed<RaftLogEntry> indexed;
    assertEquals(writer.getNextIndex(), 1);
    indexed = writer.append(new OpenSessionEntry(1, System.currentTimeMillis(), "client", "test1", "test", ReadConsistency.LINEARIZABLE, 1000));
    assertEquals(indexed.index(), 1);

    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new CloseSessionEntry(1, System.currentTimeMillis(), 1), 0));
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
    assertEquals(openSession.entry().timeout(), 1000);
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
    assertEquals(openSession.entry().timeout(), 1000);
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
    assertEquals(openSession.entry().timeout(), 1000);
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
    writer.append(new Indexed<>(2, new CloseSessionEntry(2, System.currentTimeMillis(), 1), 0));
    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);
    assertEquals(indexed.entry().term(), 2);

    // Reset the reader to a specific index and read the last entry again.
    reader.reset(2);

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
}