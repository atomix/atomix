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

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.serializer.Namespace;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

  private static final Namespace NAMESPACE = Namespace.builder()
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
      .register(ReadConsistency.class)
      .register(PrimitiveOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(Instant.class)
      .register(byte[].class)
      .build();

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
        null,
        ReadConsistency.LINEARIZABLE,
        100,
        1000));
    writer.commit(2);

    RaftServiceManager manager = raft.getServiceManager();

    manager.apply(2).join();

    Snapshot snapshot = manager.snapshot();
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot = snapshot.persist().complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    manager.install(snapshot);
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
        null,
        ReadConsistency.LINEARIZABLE,
        100,
        1000));
    writer.commit(2);

    RaftServiceManager manager = raft.getServiceManager();

    manager.apply(2).join();

    Snapshot snapshot = manager.snapshot();
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot.persist().complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    writer.append(new CommandEntry(1, System.currentTimeMillis(), 2, 1, new PrimitiveOperation(RUN, new byte[0])));
    writer.commit(3);

    manager.apply(3).join();
    assertTrue(snapshotInstalled.get());
  }

  private static final OperationId RUN = OperationId.command("run");

  private class TestService extends AbstractPrimitiveService {
    protected TestService(PrimitiveType primitiveType) {
      super(primitiveType);
    }

    @Override
    protected void configure(ServiceExecutor executor) {
      executor.register(RUN, this::run);
    }

    @Override
    public void backup(BackupOutput output) {
      output.writeLong(10);
      snapshotTaken.set(true);
    }

    @Override
    public void restore(BackupInput input) {
      assertEquals(10, input.readLong());
      snapshotInstalled.set(true);
    }

    private void run() {

    }
  }

  private class TestType implements PrimitiveType {
    @Override
    public PrimitiveConfig newConfig() {
      return null;
    }

    @Override
    public PrimitiveBuilder newBuilder(String primitiveName, PrimitiveConfig config, PrimitiveManagementService managementService) {
      return null;
    }

    @Override
    public PrimitiveService newService(ServiceConfig config) {
      return new TestService(this);
    }

    @Override
    public String name() {
      return "test";
    }
  }

  @Before
  public void setupContext() throws IOException {
    deleteStorage();

    RaftStorage storage = RaftStorage.builder()
        .withPrefix("test")
        .withDirectory(PATH.toFile())
        .withNamespace(NAMESPACE)
        .build();
    PrimitiveTypeRegistry registry = new PrimitiveTypeRegistry() {
      @Override
      public Collection<PrimitiveType> getPrimitiveTypes() {
        return Collections.singleton(new TestType());
      }

      @Override
      public PrimitiveType getPrimitiveType(String typeName) {
        return new TestType();
      }
    };
    raft = new RaftContext(
        "test",
        MemberId.from("test-1"),
        mock(ClusterMembershipService.class),
        mock(RaftServerProtocol.class),
        storage,
        registry,
        ThreadModel.SHARED_THREAD_POOL.factory("raft-server-test-%d", 1, LoggerFactory.getLogger(RaftServer.class)),
        true);

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
