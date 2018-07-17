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
package io.atomix.primitive.session.impl;

import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base class for {@link SessionClient}s that delegate to an underlying client.
 */
public class DelegatingSessionClient implements SessionClient {
  private final SessionClient session;

  public DelegatingSessionClient(SessionClient session) {
    this.session = session;
  }

  @Override
  public SessionId sessionId() {
    return session.sessionId();
  }

  @Override
  public PartitionId partitionId() {
    return session.partitionId();
  }

  @Override
  public ThreadContext context() {
    return session.context();
  }

  @Override
  public String name() {
    return session.name();
  }

  @Override
  public PrimitiveType type() {
    return session.type();
  }

  @Override
  public PrimitiveState getState() {
    return session.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    session.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    session.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return session.execute(operation);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    session.addEventListener(eventType, listener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    session.removeEventListener(eventType, listener);
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    return session.connect().thenApply(c -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return session.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("client", session)
        .toString();
  }
}
