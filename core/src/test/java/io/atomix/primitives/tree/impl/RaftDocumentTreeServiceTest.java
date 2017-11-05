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
package io.atomix.primitives.tree.impl;

import io.atomix.primitives.Ordering;
import io.atomix.primitives.tree.DocumentPath;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.impl.DefaultCommit;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.storage.StorageLevel;
import io.atomix.time.Versioned;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.Match;
import org.junit.Test;

import java.util.Optional;

import static io.atomix.primitives.tree.impl.RaftDocumentTreeOperations.GET;
import static io.atomix.primitives.tree.impl.RaftDocumentTreeOperations.UPDATE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Document tree service test.
 */
public class RaftDocumentTreeServiceTest {

  @Test
  public void testNaturalOrderedSnapshot() throws Exception {
    testSnapshot(Ordering.NATURAL);
  }

  @Test
  public void testInsertionOrderedSnapshot() throws Exception {
    testSnapshot(Ordering.INSERTION);
  }

  private void testSnapshot(Ordering ordering) throws Exception {
    SnapshotStore store = new SnapshotStore(RaftStorage.newBuilder()
        .withPrefix("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build());
    Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

    RaftDocumentTreeService service = new RaftDocumentTreeService(ordering);
    service.update(new DefaultCommit<>(
        2,
        UPDATE,
        new RaftDocumentTreeOperations.Update(
            DocumentPath.from("root|foo"),
            Optional.of("Hello world!".getBytes()),
            Match.any(),
            Match.ifNull()),
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));

    try (SnapshotWriter writer = snapshot.openWriter()) {
      service.snapshot(writer);
    }

    snapshot.complete();

    service = new RaftDocumentTreeService(ordering);
    try (SnapshotReader reader = snapshot.openReader()) {
      service.install(reader);
    }

    Versioned<byte[]> value = service.get(new DefaultCommit<>(
        2,
        GET,
        new RaftDocumentTreeOperations.Get(DocumentPath.from("root|foo")),
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));
    assertNotNull(value);
    assertArrayEquals("Hello world!".getBytes(), value.value());
  }
}
