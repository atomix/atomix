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
package io.atomix.rest.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Session registry.
 */
public class EventLog<T> implements Consumer<T> {
  private volatile EventSession<T> globalSession;
  private final Map<Integer, EventSession<T>> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger sessionId = new AtomicInteger();
  private final AtomicBoolean registered = new AtomicBoolean();

  /**
   * Returns a boolean indicating whether the event consumer needs to be registered.
   *
   * @return indicates whether the event consumer needs to be registered
   */
  boolean register() {
    return registered.compareAndSet(false, true);
  }

  /**
   * Returns a boolean indicating whether the event consumer needs to be unregistered.
   *
   * @return indicates whether the event consumer needs to be unregistered
   */
  synchronized boolean unregister() {
    return globalSession == null && sessions.isEmpty() && registered.compareAndSet(true, false);
  }

  /**
   * Returns the global session.
   *
   * @return the global session
   */
  EventSession<T> getGlobalSession() {
    EventSession<T> session = globalSession;
    if (session == null) {
      synchronized (this) {
        if (globalSession == null) {
          globalSession = new EventSession<>();
          session = globalSession;
        }
      }
    }
    return session;
  }

  /**
   * Deletes the global session.
   */
  void deleteGlobalSession() {
    EventSession<T> session = globalSession;
    if (session != null) {
      session.close();
      globalSession = null;
    }
  }

  /**
   * Returns an event session by ID.
   *
   * @param sessionId the session identifier
   * @return the event session
   */
  EventSession<T> getSession(Integer sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Deletes the given session.
   *
   * @param sessionId the session identifier
   */
  void deleteSession(int sessionId) {
    EventSession<T> session = sessions.remove(sessionId);
    if (session != null) {
      session.close();
    }
  }

  /**
   * Returns all registered sessions.
   *
   * @return all registered sessions
   */
  Collection<EventSession<T>> getSessions() {
    return sessions.values();
  }

  /**
   * Registers a new session and returns the session identifier.
   *
   * @return the registered session identifier
   */
  int newSession() {
    int sessionId = this.sessionId.incrementAndGet();
    EventSession<T> session = new EventSession<>();
    sessions.put(sessionId, session);
    return sessionId;
  }

  @Override
  public void accept(T event) {
    EventSession<T> globalSession = this.globalSession;
    if (globalSession != null) {
      globalSession.addEvent(event);
    }
    sessions.values().forEach(session -> session.addEvent(event));
  }
}
