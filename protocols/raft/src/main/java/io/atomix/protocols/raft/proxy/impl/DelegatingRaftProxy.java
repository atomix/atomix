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
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.collect.Maps;
import io.atomix.protocols.raft.event.EventType;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default Raft proxy.
 */
public class DelegatingRaftProxy implements RaftProxy {
  private final RaftProxyClient client;
  private final Map<EventType, Map<Object, Consumer<RaftEvent>>> eventTypeListeners = Maps.newConcurrentMap();

  public DelegatingRaftProxy(RaftProxyClient client) {
    this.client = client;
  }

  @Override
  public SessionId sessionId() {
    return client.sessionId();
  }

  @Override
  public String name() {
    return client.name();
  }

  @Override
  public ServiceType serviceType() {
    return client.serviceType();
  }

  @Override
  public ServiceRevision revision() {
    return client.revision();
  }

  @Override
  public State getState() {
    return client.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    client.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    client.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return client.execute(operation);
  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {
    client.addEventListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {
    client.removeEventListener(listener);
  }

  @Override
  public void addEventListener(EventType eventType, Runnable listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.run();
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<byte[]> listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.accept(e.value());
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.accept(decoder.apply(e.value()));
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Runnable listener) {
    Consumer<RaftEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer listener) {
    Consumer<RaftEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public CompletableFuture<RaftProxy> open() {
    return client.open().thenApply(c -> this);
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("client", client)
        .toString();
  }
}
