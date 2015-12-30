/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.manager;

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.copycat.client.session.Session;
import io.atomix.resource.InstanceEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ManagedResourceSession implements Session {
  private final long resource;
  private final Session parent;
  private final Map<String, Listeners<Object>> eventListeners = new ConcurrentHashMap<>();

  public ManagedResourceSession(long resource, Session parent) {
    this.resource = resource;
    this.parent = parent;
  }

  @Override
  public long id() {
    return resource;
  }

  @Override
  public State state() {
    return parent.state();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return parent.onStateChange(callback);
  }

  @Override
  public Session publish(String event) {
    return parent.publish(event, new InstanceEvent<>(resource, null));
  }

  @Override
  public Session publish(String event, Object message) {
    return parent.publish(event, new InstanceEvent<>(resource, message));
  }

  /**
   * handles receiving an event.
   */
  @SuppressWarnings("unchecked")
  private void handleEvent(String event, InstanceEvent message) {
    if (message.resource() == resource) {
      Listeners<Object> listeners = eventListeners.get(event);
      if (listeners != null) {
        for (Consumer listener : listeners) {
          listener.accept(message.message());
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized Listener onEvent(String event, Consumer listener) {
    Listeners listeners = eventListeners.get(event);
    if (listeners == null) {
      listeners = new Listeners();
      eventListeners.put(event, listeners);
      parent.onEvent(event, message -> handleEvent(event, (InstanceEvent) message));
    }
    return listeners.add(listener);
  }

}
