/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy;

import com.google.common.collect.Maps;
import io.atomix.protocols.raft.EventType;
import io.atomix.protocols.raft.RaftEvent;
import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.session.SessionId;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate.
 */
public class DelegatingRaftProxy implements RaftProxy {
  private final RaftProxy delegate;
  private final Map<EventType, Map<Object, Consumer<RaftEvent>>> typedEventListeners = Maps.newConcurrentMap();

  public DelegatingRaftProxy(RaftProxy delegate) {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public String typeName() {
    return delegate.typeName();
  }

  @Override
  public SessionId sessionId() {
    return delegate.sessionId();
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    delegate.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    delegate.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return delegate.execute(operation);
  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {
    delegate.addEventListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {
    delegate.removeEventListener(listener);
  }

  @Override
  public void addEventListener(EventType eventType, Runnable listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.run();
      }
    };
    typedEventListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<byte[]> listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.accept(e.value());
      }
    };
    typedEventListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    Consumer<RaftEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.accept(decoder.apply(e.value()));
      }
    };
    typedEventListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Runnable listener) {
    Consumer<RaftEvent> eventListener =
        typedEventListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer listener) {
    Consumer<RaftEvent> eventListener =
        typedEventListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return delegate.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
