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
package io.atomix.primitive.client.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.client.SessionClient;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default Raft proxy.
 */
public class DelegatingSessionClient implements SessionClient {
  private final SessionClient proxy;
  private final Map<String, Map<Object, Consumer<PrimitiveEvent>>> eventTypeListeners = Maps.newConcurrentMap();

  public DelegatingSessionClient(SessionClient proxy) {
    this.proxy = proxy;
  }

  @Override
  public SessionId sessionId() {
    return proxy.sessionId();
  }

  @Override
  public PartitionId partitionId() {
    return proxy.partitionId();
  }

  @Override
  public ThreadContext context() {
    return proxy.context();
  }

  @Override
  public String name() {
    return proxy.name();
  }

  @Override
  public PrimitiveType type() {
    return proxy.type();
  }

  @Override
  public PrimitiveState getState() {
    return proxy.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    proxy.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    proxy.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return proxy.execute(operation);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    proxy.addEventListener(eventType, listener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    proxy.removeEventListener(eventType, listener);
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    return proxy.connect().thenApply(c -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return proxy.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("client", proxy)
        .toString();
  }
}
