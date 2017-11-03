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
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Raft session manager test.
 */
public class RaftSessionRegistryTest {

  /**
   * Tests that the same session can be registered twice without replacing the original session.
   */
  @Test
  public void testRegisterIdempotent() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    RaftSessionContext session1 = createSession(1);
    RaftSessionContext session2 = createSession(1);
    sessionManager.registerSession(session1);
    sessionManager.registerSession(session2);
    assertSame(session1, sessionManager.getSession(1));
  }

  @Test
  public void testUnregisterSession() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    RaftSessionContext session = createSession(1);
    sessionManager.registerSession(session);
    assertNotNull(sessionManager.getSession(1));
    assertEquals(1, sessionManager.getSessions(ServiceId.from(1)).size());
    sessionManager.closeSession(SessionId.from(1));
    assertNull(sessionManager.getSession(1));
  }

  @Test
  public void testSessionListeners() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    TestSessionListener listener = new TestSessionListener();
    sessionManager.addListener(ServiceId.from(1), listener);

    RaftSessionContext session1 = createSession(1);
    sessionManager.registerSession(session1);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessionManager.closeSession(session1.sessionId());
    assertTrue(listener.eventReceived());
    assertTrue(listener.isClosed());

    RaftSessionContext session2 = createSession(2);
    sessionManager.registerSession(session2);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessionManager.expireSession(session2.sessionId());
    assertTrue(listener.eventReceived());
    assertTrue(listener.isExpired());
    sessionManager.expireSession(session2.sessionId());
    assertFalse(listener.eventReceived());

    RaftSessionContext session3 = createSession(3);
    sessionManager.registerSession(session3);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessionManager.registerSession(session3);
    assertFalse(listener.eventReceived());
  }

  private RaftSessionContext createSession(long sessionId) {
    DefaultServiceContext context = mock(DefaultServiceContext.class);
    when(context.serviceType()).thenReturn(ServiceType.from("test"));
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(ServiceId.from(1));
    when(context.executor()).thenReturn(mock(ThreadContext.class));

    RaftContext server = mock(RaftContext.class);
    when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));

    return new RaftSessionContext(
        SessionId.from(sessionId),
        MemberId.from("1"),
        "test",
        ServiceType.from("test"),
        ReadConsistency.LINEARIZABLE,
        100,
        5000,
        context,
        server,
        mock(ThreadContextFactory.class));
  }

  private class TestSessionListener implements RaftSessionListener {
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void onOpen(RaftSession session) {
      queue.add("open");
    }

    @Override
    public void onExpire(RaftSession session) {
      queue.add("expire");
    }

    @Override
    public void onClose(RaftSession session) {
      queue.add("close");
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public String event() throws InterruptedException {
      return queue.take();
    }

    public boolean isOpened() throws InterruptedException {
      return event().equals("open");
    }

    public boolean isClosed() throws InterruptedException {
      return event().equals("close");
    }

    public boolean isExpired() throws InterruptedException {
      return event().equals("expire");
    }
  }
}
