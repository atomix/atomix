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
package io.atomix.primitives.queue.impl;

import io.atomix.primitives.queue.Task;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.impl.DefaultCommit;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.storage.StorageLevel;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.AtomixThreadFactory;
import io.atomix.utils.concurrent.SingleThreadContextFactory;
import io.atomix.utils.concurrent.ThreadContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static io.atomix.primitives.DistributedPrimitive.Type.WORK_QUEUE;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.ADD;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.TAKE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Work queue service test.
 */
public class RaftWorkQueueServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    SnapshotStore store = new SnapshotStore(RaftStorage.newBuilder()
        .withPrefix("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build());
    Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

    DefaultServiceContext context = mock(DefaultServiceContext.class);
    when(context.serviceType()).thenReturn(ServiceType.from(WORK_QUEUE.name()));
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(ServiceId.from(1));
    when(context.executor()).thenReturn(mock(ThreadContext.class));

    RaftContext server = mock(RaftContext.class);
    when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));

    RaftSessionContext session = new RaftSessionContext(
        SessionId.from(1),
        MemberId.from("1"),
        "test",
        ServiceType.from(WORK_QUEUE.name()),
        ReadConsistency.LINEARIZABLE,
        100,
        5000,
        context,
        server,
        new SingleThreadContextFactory(new AtomixThreadFactory()));

    RaftWorkQueueService service = new RaftWorkQueueService();
    service.init(context);

    service.add(new DefaultCommit<>(
        2,
        ADD,
        new RaftWorkQueueOperations.Add(Arrays.asList("Hello world!".getBytes())),
        session,
        System.currentTimeMillis()));

    try (SnapshotWriter writer = snapshot.openWriter()) {
      service.snapshot(writer);
    }

    snapshot.complete();

    service = new RaftWorkQueueService();
    service.init(context);

    try (SnapshotReader reader = snapshot.openReader()) {
      service.install(reader);
    }

    Collection<Task<byte[]>> value = service.take(new DefaultCommit<>(
        2,
        TAKE,
        new RaftWorkQueueOperations.Take(1),
        session,
        System.currentTimeMillis()));
    assertNotNull(value);
    assertEquals(1, value.size());
    assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
  }
}
