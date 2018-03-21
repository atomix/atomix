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

import io.atomix.protocols.raft.service.AbstractRaftService;
import io.atomix.protocols.raft.service.RaftServiceExecutor;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.PropagationStrategy;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Service registry test.
 */
public class RaftServiceRegistryTest {

  @Test
  public void testServiceRegistry() throws Exception {
    RaftServiceRegistry registry = new RaftServiceRegistry();
    registry.registerService(createService("foo", 1));
    assertEquals(1, registry.getCurrentRevision("foo").revision().revision());

    registry.registerService(createService("foo", 2));
    assertEquals(2, registry.getCurrentRevision("foo").revision().revision());
    assertEquals(2, registry.getRevisions("foo").size());
  }

  private DefaultServiceContext createService(String name, int revision) {
    return new DefaultServiceContext(
        ServiceId.from(1),
        name,
        ServiceType.from("test"),
        new ServiceRevision(revision, PropagationStrategy.NONE),
        new AbstractRaftService() {
          @Override
          protected void configure(RaftServiceExecutor executor) {

          }

          @Override
          public void snapshot(SnapshotWriter writer) {

          }

          @Override
          public void install(SnapshotReader reader) {

          }
        },
        mock(RaftContext.class),
        mock(ThreadContextFactory.class));
  }

}
