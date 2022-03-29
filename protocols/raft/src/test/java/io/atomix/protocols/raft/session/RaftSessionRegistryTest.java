// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.session;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.TestPrimitiveType;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
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
    RaftSession session = createSession(1);
    sessionManager.addSession(session);
    assertNotNull(sessionManager.getSession(1));
    assertNotNull(sessionManager.getSession(session.sessionId()));
    assertEquals(0, sessionManager.getSessions(PrimitiveId.from(1)).size());
    session.open();
    assertEquals(1, sessionManager.getSessions(PrimitiveId.from(1)).size());
    sessionManager.removeSession(SessionId.from(1));
    assertNull(sessionManager.getSession(1));
  }

  private RaftSession createSession(long sessionId) {
    RaftServiceContext context = mock(RaftServiceContext.class);
    when(context.serviceType()).thenReturn(TestPrimitiveType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));

    RaftContext server = mock(RaftContext.class);
    when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));
    RaftServiceManager manager = mock(RaftServiceManager.class);
    when(manager.executor()).thenReturn(mock(ThreadContext.class));
    when(server.getServiceManager()).thenReturn(manager);

    return new RaftSession(
        SessionId.from(sessionId),
        MemberId.from("1"),
        "test",
        TestPrimitiveType.instance(),
        ReadConsistency.LINEARIZABLE,
        100,
        5000,
        System.currentTimeMillis(),
        Serializer.using(Namespaces.BASIC),
        context,
        server,
        mock(ThreadContextFactory.class));
  }
}
