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
package io.atomix.protocols.raft.service.impl;

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.impl.RaftServiceManager;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSession.State;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.session.impl.RaftSessionRegistry;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Default service sessions test.
 */
public class DefaultServiceSessionsTest {
  @Test
  public void testSessions() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    DefaultServiceSessions sessions = new DefaultServiceSessions(ServiceId.from(1), sessionManager);
    TestSessionListener listener = new TestSessionListener();
    sessions.addListener(listener);

    RaftSessionContext session1 = createSession(1);
    sessionManager.addSession(session1);
    assertNull(sessions.getSession(1));
    sessions.openSession(session1);
    assertNotNull(sessions.getSession(1));
    assertEquals(session1.getState(), State.OPEN);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessions.closeSession(session1);
    assertEquals(session1.getState(), State.CLOSED);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isClosed());

    RaftSessionContext session2 = createSession(2);
    sessions.openSession(session2);
    assertEquals(session2.getState(), State.OPEN);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessions.expireSession(session2);
    assertEquals(session2.getState(), State.EXPIRED);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isExpired());
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
