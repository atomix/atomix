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
package io.atomix.rest.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Event subject.
 */
class EventSubject extends EventSession implements Consumer<String> {
  private final Map<Integer, EventSession> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger sessionId = new AtomicInteger();

  EventSession getSession(int sessionId) {
    return sessions.get(sessionId);
  }

  int newSession() {
    int sessionId = this.sessionId.incrementAndGet();
    EventSession session = new EventSession();
    sessions.put(sessionId, session);
    return sessionId;
  }

  @Override
  public void accept(String event) {
    addEvent(event);
    sessions.values().forEach(s -> s.addEvent(event));
  }
}