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
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.proxy.PrimitiveProxy;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default Raft proxy.
 */
public abstract class AbstractPrimitiveProxy implements PrimitiveProxy {
  private final Map<EventType, Map<Object, Consumer<PrimitiveEvent>>> eventTypeListeners = Maps.newConcurrentMap();

  @Override
  public void addEventListener(EventType eventType, Runnable listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.run();
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<byte[]> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.accept(e.value());
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    Consumer<PrimitiveEvent> wrappedListener = e -> {
      if (e.type().equals(eventType)) {
        listener.accept(decoder.apply(e.value()));
      }
    };
    eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
    addEventListener(wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Runnable listener) {
    Consumer<PrimitiveEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer listener) {
    Consumer<PrimitiveEvent> eventListener =
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
            .remove(listener);
    removeEventListener(eventListener);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", sessionId())
        .toString();
  }
}
