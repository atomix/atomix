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
package io.atomix.protocols.backup.impl;

import com.google.common.collect.Sets;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.primitive.session.PrimitiveSessionEvent;
import io.atomix.primitive.session.PrimitiveSessionEvent.Type;
import io.atomix.primitive.session.PrimitiveSessionEventListener;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Set;

/**
 * Primary-backup session.
 */
public class PrimaryBackupSession implements PrimitiveSession {
  private final Logger log;
  private final SessionId sessionId;
  private final MemberId memberId;
  private final Serializer serializer;
  private final PrimaryBackupServiceContext context;
  private final Set<PrimitiveSessionEventListener> eventListeners = Sets.newIdentityHashSet();
  private State state = State.OPEN;

  public PrimaryBackupSession(SessionId sessionId, MemberId memberId, Serializer serializer, PrimaryBackupServiceContext context) {
    this.sessionId = sessionId;
    this.memberId = memberId;
    this.serializer = serializer;
    this.context = context;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(context.serverName())
        .add("session", sessionId)
        .build());
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String serviceName() {
    return context.serviceName();
  }

  @Override
  public PrimitiveType serviceType() {
    return context.serviceType();
  }

  @Override
  public MemberId memberId() {
    return memberId;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addListener(PrimitiveSessionEventListener listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(PrimitiveSessionEventListener listener) {
    eventListeners.remove(listener);
  }

  @Override
  public <T> void publish(EventType eventType, T event) {
    publish(PrimitiveEvent.event(eventType, serializer.encode(event)));
  }

  @Override
  public void publish(PrimitiveEvent event) {
    if (context.getRole() == Role.PRIMARY) {
      context.threadContext().execute(() -> {
        log.trace("Sending {} to {}", event, memberId);
        context.protocol().event(memberId, sessionId, event);
      });
    }
  }

  public void expire() {
    state = State.EXPIRED;
    eventListeners.forEach(l -> l.onEvent(new PrimitiveSessionEvent(Type.EXPIRE, this)));
  }

  public void close() {
    state = State.CLOSED;
    eventListeners.forEach(l -> l.onEvent(new PrimitiveSessionEvent(Type.CLOSE, this)));
  }
}
