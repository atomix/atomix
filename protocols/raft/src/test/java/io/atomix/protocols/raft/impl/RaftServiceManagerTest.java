/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.ThreadModel;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.operation.impl.DefaultOperationId;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.PropagationStrategy;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.log.TestEntry;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Raft service manager test.
 */
public class RaftServiceManagerTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
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
      .register(MemberId.class)
      .register(RaftMember.Type.class)
      .register(ReadConsistency.class)
      .register(PropagationStrategy.class)
      .register(RaftOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(Instant.class)
      .register(byte[].class)
      .build());

  private RaftContext raft;
  private AtomicBoolean snapshotTaken;
  private AtomicBoolean snapshotInstalled;

  @Test
  public void testSnapshotTakeInstall() throws Exception {
    RaftLogWriter writer = raft.getLogWriter();
    writer.append(new InitializeEntry(1, System.currentTimeMillis()));
    writer.append(new OpenSessionEntry(
        1,
        System.currentTimeMillis(),
        "test-1",
        "test",
        "test",
        ReadConsistency.LINEARIZABLE,
        100,
        1000,
        1,
        PropagationStrategy.NONE));
    writer.commit(2);

    RaftServiceManager manager = raft.getServiceManager();

    manager.apply(2).join();

    assertEquals(1, raft.getServices().getCurrentRevision("test").revision().revision());
    assertEquals(2, (long) raft.getServices().getCurrentRevision("test").sessions().getSession(2).sessionId().id());

    Snapshot snapshot = manager.snapshot(2);
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot.persist().complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    manager.install(3);
    assertTrue(snapshotInstalled.get());
  }

  @Test
  public void testInstallSnapshotOnApply() throws Exception {
    RaftLogWriter writer = raft.getLogWriter();
    writer.append(new InitializeEntry(1, System.currentTimeMillis()));
    writer.append(new OpenSessionEntry(
        1,
        System.currentTimeMillis(),
        "test-1",
        "test",
        "test",
        ReadConsistency.LINEARIZABLE,
        100,
        1000,
        1,
        PropagationStrategy.NONE));
    writer.commit(2);

    RaftServiceManager manager = raft.getServiceManager();

    manager.apply(2).join();

    assertEquals(1, raft.getServices().getCurrentRevision("test").revision().revision());
    assertEquals(2, (long) raft.getServices().getCurrentRevision("test").sessions().getSession(2).sessionId().id());

    Snapshot snapshot = manager.snapshot(2);
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot.persist().complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    writer.append(new CommandEntry(1, System.currentTimeMillis(), 2, 1, new RaftOperation(RUN, new byte[0])));
    writer.commit(3);

    manager.apply(3).join();
    assertTrue(snapshotInstalled.get());
  }

  private static final OperationId RUN = OperationId.command("run");

  private class TestService extends AbstractRaftService {
    @Override
    protected void configure(RaftServiceExecutor executor) {
      executor.register(RUN, this::run);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      writer.writeLong(10);
      snapshotTaken.set(true);
    }

    @Override
    public void install(SnapshotReader reader) {
      assertEquals(10, reader.readLong());
      snapshotInstalled.set(true);
    }

    private void run() {

    }
  }

  @Before
  public void setupContext() throws IOException {
    deleteStorage();

    RaftStorage storage = RaftStorage.newBuilder()
        .withPrefix("test")
        .withDirectory(PATH.toFile())
        .withSerializer(SERIALIZER)
        .build();
    RaftServiceFactoryRegistry registry = new RaftServiceFactoryRegistry();
    registry.register("test", TestService::new);
    raft = new RaftContext(
        "test",
        MemberId.from("test-1"),
        mock(RaftServerProtocol.class),
        storage,
        registry,
        ThreadModel.SHARED_THREAD_POOL,
        1);

    snapshotTaken = new AtomicBoolean();
    snapshotInstalled = new AtomicBoolean();
  }

  @After
  public void teardownContext() throws IOException {
    raft.close();
    deleteStorage();
  }

  private void deleteStorage() throws IOException {
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
