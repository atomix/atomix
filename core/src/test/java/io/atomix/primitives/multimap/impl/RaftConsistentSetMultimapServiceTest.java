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
package io.atomix.primitives.multimap.impl;

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

import java.util.Arrays;
import java.util.Collection;

import static io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapOperations.GET;
import static io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapOperations.PUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Consistent set multimap service test.
 */
public class RaftConsistentSetMultimapServiceTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    SnapshotStore store = new SnapshotStore(RaftStorage.newBuilder()
        .withPrefix("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build());
    Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

    RaftConsistentSetMultimapService service = new RaftConsistentSetMultimapService();
    service.put(new DefaultCommit<>(
        2,
        PUT,
        new RaftConsistentSetMultimapOperations.Put(
            "foo", Arrays.asList("Hello world!".getBytes()), Match.ANY),
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));

    try (SnapshotWriter writer = snapshot.openWriter()) {
      service.snapshot(writer);
    }

    snapshot.complete();

    service = new RaftConsistentSetMultimapService();
    try (SnapshotReader reader = snapshot.openReader()) {
      service.install(reader);
    }

    Versioned<Collection<? extends byte[]>> value = service.get(new DefaultCommit<>(
        2,
        GET,
        new RaftConsistentSetMultimapOperations.Get("foo"),
        mock(RaftSessionContext.class),
        System.currentTimeMillis()));
    assertNotNull(value);
    assertEquals(1, value.value().size());
    assertArrayEquals("Hello world!".getBytes(), value.value().iterator().next());
  }
}
