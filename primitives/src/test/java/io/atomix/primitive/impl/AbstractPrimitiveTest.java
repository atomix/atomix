/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitive.impl;

import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.RaftEvent;
import io.atomix.primitive.operation.RaftOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.AbstractPrimitiveProxy;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionListener;
import io.atomix.primitive.session.Sessions;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import org.junit.After;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for various Atomix tests.
 *
 * @param <T> the Raft primitive type being tested
 */
public abstract class AbstractPrimitiveTest<T extends AbstractRaftPrimitive> {
  private Map<String, TestPrimitiveService> services = new ConcurrentHashMap<>();
  private final ThreadContext context = new SingleThreadContext("atomix-test-%d");

  /**
   * Creates the primitive service.
   *
   * @return the primitive service
   */
  protected abstract PrimitiveService createService();

  /**
   * Creates a new primitive.
   *
   * @param name the primitive name
   * @return the primitive instance
   */
  protected T newPrimitive(String name) {
    return createPrimitive(createProxy(name).open().join());
  }

  /**
   * Creates a new primitive instance.
   *
   * @param proxy the primitive proxy
   * @return the primitive instance
   */
  protected abstract T createPrimitive(PrimitiveProxy proxy);

  /**
   * Creates a new primitive proxy for the given primitive service.
   *
   * @param name the primitive name
   * @return the primitive proxy
   */
  private PrimitiveProxy createProxy(String name) {
    return services.computeIfAbsent(name, TestPrimitiveService::new).createProxy();
  }

  @After
  public void clear() {
    services.values().forEach(TestPrimitiveService::close);
    services.clear();
  }

  /**
   * Test primitive service.
   */
  private class TestPrimitiveService {
    private final AtomicLong nextSessionId = new AtomicLong();
    private final AtomicLong nextIndex = new AtomicLong();
    private final TestSessions sessions = new TestSessions();
    private final PrimitiveService service;
    private final Scheduled tickTimer;

    TestPrimitiveService(String name) {
      this.service = createService();
      ServiceContext context = mock(ServiceContext.class);
      when(context.currentIndex()).thenReturn(nextIndex.get());
      when(context.sessions()).thenReturn(sessions);
      when(context.wallClock()).thenReturn(new WallClock());
      service.init(context);
      this.tickTimer = AbstractPrimitiveTest.this.context.schedule(
          Duration.ofMillis(10),
          Duration.ofMillis(10),
          () -> service.tick(new WallClockTimestamp()));
    }

    /**
     * Creates a new primitive proxy.
     */
    PrimitiveProxy createProxy() {
      return new TestPrimitiveProxy(service);
    }

    /**
     * Closes the service.
     */
    void close() {
      tickTimer.cancel();
    }

    /**
     * Test primitive proxy.
     */
    private class TestPrimitiveProxy extends AbstractPrimitiveProxy {
      private final Set<Consumer<State>> stateChangeListeners = Sets.newIdentityHashSet();
      private final Set<Consumer<RaftEvent>> eventListeners = Sets.newIdentityHashSet();
      private final PrimitiveService service;
      private final Session session;
      private final AtomicBoolean open = new AtomicBoolean();

      public TestPrimitiveProxy(PrimitiveService service) {
        this.service = service;
        this.session = mockSession();
      }

      private Session mockSession() {
        Session session = mock(Session.class);
        when(session.sessionId()).thenReturn(SessionId.from(nextSessionId.incrementAndGet()));
        when(session.getState()).thenAnswer(i -> isOpen() ? Session.State.OPEN : Session.State.CLOSED);
        doAnswer(invocation -> {
          EventType eventType = invocation.getArgumentAt(0, EventType.class);
          Function<Object, byte[]> encoder = invocation.getArgumentAt(1, Function.class);
          Object event = invocation.getArgumentAt(2, Object.class);
          eventListeners.forEach(l -> l.accept(new RaftEvent(eventType, encoder.apply(event))));
          return null;
        }).when(session).publish(any(EventType.class), any(Function.class), any());
        doAnswer(invocation -> {
          EventType eventType = invocation.getArgumentAt(0, EventType.class);
          byte[] event = invocation.getArgumentAt(1, byte[].class);
          eventListeners.forEach(l -> l.accept(new RaftEvent(eventType, event)));
          return null;
        }).when(session).publish(any(EventType.class), any(byte[].class));
        doAnswer(invocation -> {
          RaftEvent event = invocation.getArgumentAt(0, RaftEvent.class);
          eventListeners.forEach(l -> l.accept(event));
          return null;
        }).when(session).publish(any(RaftEvent.class));
        return session;
      }

      @Override
      public SessionId sessionId() {
        return session.sessionId();
      }

      @Override
      public String name() {
        return null;
      }

      @Override
      public PrimitiveType serviceType() {
        return null;
      }

      @Override
      public State getState() {
        return isOpen() ? State.CONNECTED : State.CLOSED;
      }

      @Override
      public void addStateChangeListener(Consumer<State> listener) {
        stateChangeListeners.add(listener);
      }

      @Override
      public void removeStateChangeListener(Consumer<State> listener) {
        stateChangeListeners.remove(listener);
      }

      @Override
      public CompletableFuture<byte[]> execute(RaftOperation operation) {
        return CompletableFuture.completedFuture(service.apply(new DefaultCommit<>(
            nextIndex.incrementAndGet(),
            operation.id(),
            operation.value(),
            session,
            System.currentTimeMillis())));
      }

      @Override
      public void addEventListener(Consumer<RaftEvent> listener) {
        eventListeners.add(listener);
      }

      @Override
      public void removeEventListener(Consumer<RaftEvent> listener) {
        eventListeners.remove(listener);
      }

      @Override
      public CompletableFuture<PrimitiveProxy> open() {
        if (open.compareAndSet(false, true)) {
          sessions.addSession(session);
          stateChangeListeners.forEach(l -> l.accept(State.CONNECTED));
        }
        return CompletableFuture.completedFuture(this);
      }

      @Override
      public boolean isOpen() {
        return open.get();
      }

      @Override
      public CompletableFuture<Void> close() {
        if (open.compareAndSet(true, false)) {
          stateChangeListeners.forEach(l -> l.accept(State.CLOSED));
          sessions.removeSession(session.sessionId());
          service.onClose(session);
        }
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public boolean isClosed() {
        return !open.get();
      }
    }
  }

  /**
   * Test sessions.
   */
  private class TestSessions implements Sessions {
    private Map<Long, Session> sessions = new ConcurrentHashMap<>();
    private final Set<SessionListener> listeners = Sets.newIdentityHashSet();

    @Override
    public Session getSession(long sessionId) {
      return sessions.get(sessionId);
    }

    @Override
    public Sessions addListener(SessionListener listener) {
      listeners.add(listener);
      return this;
    }

    @Override
    public Sessions removeListener(SessionListener listener) {
      listeners.remove(listener);
      return this;
    }

    void addSession(Session session) {
      sessions.put(session.sessionId().id(), session);
      listeners.forEach(l -> l.onOpen(session));
    }

    void removeSession(SessionId sessionId) {
      Session session = sessions.remove(sessionId.id());
      if (session != null) {
        listeners.forEach(l -> l.onClose(session));
      }
    }

    @Override
    public Iterator<Session> iterator() {
      return sessions.values().iterator();
    }
  }
}
