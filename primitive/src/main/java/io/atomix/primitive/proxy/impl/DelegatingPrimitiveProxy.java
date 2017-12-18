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
package io.atomix.primitive.proxy.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.SessionId;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default Raft proxy.
 */
public class DelegatingPrimitiveProxy implements PrimitiveProxy {
  private final PrimitiveProxy proxy;
  private final Map<String, Map<Object, Consumer<PrimitiveEvent>>> eventTypeListeners = Maps.newConcurrentMap();

  public DelegatingPrimitiveProxy(PrimitiveProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public SessionId sessionId() {
    return proxy.sessionId();
  }

  @Override
  public String name() {
    return proxy.name();
  }

  @Override
  public PrimitiveType serviceType() {
    return proxy.serviceType();
  }

  @Override
  public State getState() {
    return proxy.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    proxy.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    proxy.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return proxy.execute(operation);
  }

  @Override
  public void addEventListener(Consumer<PrimitiveEvent> listener) {
    proxy.addEventListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<PrimitiveEvent> listener) {
    proxy.removeEventListener(listener);
  }

  @Override
  public void addEventListener(EventType eventType, Runnable listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.run();
      }
    };
    eventTypeListeners.computeIfAbsent(eventType.id(), e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<byte[]> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.accept(e.value());
      }
    };
    eventTypeListeners.computeIfAbsent(eventType.id(), e -> Maps.newConcurrentMap())
        .put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().id().equals(eventType.id())) {
        listener.accept(decoder.apply(e.value()));
      }
    };
    eventTypeListeners.computeIfAbsent(eventType.id(), e -> Maps.newConcurrentMap())
        .put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Runnable listener) {
    Consumer<PrimitiveEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType.id(), e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer listener) {
    Consumer<PrimitiveEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType.id(), e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public CompletableFuture<PrimitiveProxy> connect() {
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
