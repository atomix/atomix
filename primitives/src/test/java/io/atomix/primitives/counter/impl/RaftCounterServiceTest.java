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
package io.atomix.primitives.counter.impl;

import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.impl.DefaultCommit;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.storage.StorageLevel;
import io.atomix.time.WallClockTimestamp;
import org.junit.Test;

import static io.atomix.primitives.counter.impl.RaftCounterOperations.GET;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.SET;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Counter service test.
 */
public class RaftCounterServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    SnapshotStore store = new SnapshotStore(RaftStorage.newBuilder()
        .withPrefix("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build());
    Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

    RaftCounterService service = new RaftCounterService();
    service.set(new DefaultCommit<>(
        2,
        SET,
        new RaftCounterOperations.Set(1L),
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));

    try (SnapshotWriter writer = snapshot.openWriter()) {
      service.snapshot(writer);
    }

    snapshot.complete();

    service = new RaftCounterService();
    try (SnapshotReader reader = snapshot.openReader()) {
      service.install(reader);
    }

    long value = service.get(new DefaultCommit<>(
        2,
        GET,
        null,
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));
    assertEquals(1, value);
  }
}
