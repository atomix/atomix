/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.session.impl;

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Raft session manager test.
 */
public class RaftSessionRegistryTest {

  @Test
  public void testAddRemoveSession() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    RaftSessionContext session = createSession(1);
    sessionManager.addSession(session);
    assertNotNull(sessionManager.getSession(1));
    assertEquals(0, sessionManager.getSessions(ServiceId.from(1)).size());
    session.open();
    assertEquals(1, sessionManager.getSessions(ServiceId.from(1)).size());
    sessionManager.removeSession(SessionId.from(1));
    assertNull(sessionManager.getSession(1));
  }

  private RaftSessionContext createSession(long sessionId) {
    DefaultServiceContext context = mock(DefaultServiceContext.class);
    when(context.serviceType()).thenReturn(ServiceType.from("test"));
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(ServiceId.from(1));

    RaftContext server = mock(RaftContext.class);
    when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));
    RaftServiceManager manager = mock(RaftServiceManager.class);
    when(manager.executor()).thenReturn(mock(ThreadContext.class));
    when(server.getServiceManager()).thenReturn(manager);

    return new RaftSessionContext(
        SessionId.from(sessionId),
        MemberId.from("1"),
        "test",
        ServiceType.from("test"),
        ReadConsistency.LINEARIZABLE,
        100,
        5000,
        System.currentTimeMillis(),
        context,
        server,
        mock(ThreadContextFactory.class));
  }
}
