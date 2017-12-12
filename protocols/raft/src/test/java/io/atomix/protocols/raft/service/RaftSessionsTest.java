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
package io.atomix.protocols.raft.service;

import io.atomix.cluster.NodeId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.Session.State;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionListener;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.proxy.impl.TestPrimitiveType;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionRegistry;
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
public class RaftSessionsTest {
  @Test
  public void testSessions() throws Exception {
    RaftSessionRegistry sessionManager = new RaftSessionRegistry();
    RaftSessions sessions = new RaftSessions(PrimitiveId.from(1), sessionManager);
    TestSessionListener listener = new TestSessionListener();
    sessions.addListener(listener);

    RaftSession session1 = createSession(1);
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

    RaftSession session2 = createSession(2);
    sessions.openSession(session2);
    assertEquals(session2.getState(), State.OPEN);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isOpened());
    sessions.expireSession(session2);
    assertEquals(session2.getState(), State.EXPIRED);
    assertTrue(listener.eventReceived());
    assertTrue(listener.isExpired());
  }

  private RaftSession createSession(long sessionId) {
    RaftServiceContext context = mock(RaftServiceContext.class);
    when(context.serviceType()).thenReturn(new TestPrimitiveType());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.executor()).thenReturn(mock(ThreadContext.class));

    RaftContext server = mock(RaftContext.class);
    when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));

    return new RaftSession(
        SessionId.from(sessionId),
        NodeId.from("1"),
        "test",
        new TestPrimitiveType(),
        ReadConsistency.LINEARIZABLE,
        100,
        5000,
        System.currentTimeMillis(),
        context,
        server,
        mock(ThreadContextFactory.class));
  }

  private class TestSessionListener implements SessionListener {
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void onOpen(Session session) {
      queue.add("open");
    }

    @Override
    public void onExpire(Session session) {
      queue.add("expire");
    }

    @Override
    public void onClose(Session session) {
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
