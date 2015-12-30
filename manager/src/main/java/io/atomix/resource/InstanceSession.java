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
package io.atomix.resource;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.session.Session;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class InstanceSession implements Session {
  private final long resource;
  private final Session parent;
  private final ThreadContext context;
  private final Map<String, Set<EventListener>> eventListeners = new ConcurrentHashMap<>();
  private final Map<String, Listener<InstanceEvent<?>>> listeners = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if {@code parent} or {@code context} are null
   */
  public InstanceSession(long resource, Session parent, ThreadContext context) {
    this.resource = resource;
    this.parent = Assert.notNull(parent, "parent");
    this.context = Assert.notNull(context, "context");
  }

  @Override
  public long id() {
    return parent.id();
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
    return publish(event, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Session publish(String event, Object message) {
    Set<EventListener> listeners = eventListeners.get(event);
    if (listeners != null) {
      context.executor().execute(() -> {
        for (Consumer<Object> listener : listeners) {
          listener.accept(message);
        }
      });
    }
    return this;
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> Listener<T> onEvent(String event, Consumer<T> listener) {
    Assert.notNull(event, "event");
    Assert.notNull(listener, "listener");

    Set<EventListener> listeners = eventListeners.get(event);
    if (listeners == null) {
      listeners = new HashSet<>();
      eventListeners.put(event, listeners);
      this.listeners.put(event, parent.onEvent(event, message -> handleEvent(event, message)));
    }

    EventListener context = new EventListener(event, listener);
    listeners.add(context);
    return context;
  }

  /**
   * Handles receiving a resource message.
   */
  @SuppressWarnings("unchecked")
  private void handleEvent(String event, InstanceEvent<?> message) {
    if (message.resource() == resource) {
      Set<EventListener> listeners = eventListeners.get(event);
      if (listeners != null) {
        for (EventListener listener : listeners) {
          listener.accept(message.message());
        }
      }
    }
  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return parent.equals(object);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, resource=%d]", getClass().getSimpleName(), id(), resource);
  }

  /**
   * Receive listener context.
   */
  private class EventListener<T> implements Listener<T> {
    private final String event;
    private final Consumer<T> listener;

    private EventListener(String event, Consumer<T> listener) {
      this.event = event;
      this.listener = listener;
    }

    @Override
    public void accept(T event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      synchronized (InstanceSession.this) {
        Set<EventListener> listeners = eventListeners.get(event);
        if (listeners != null) {
          listeners.remove(this);
          if (listeners.isEmpty()) {
            eventListeners.remove(event);
            Listener listener = InstanceSession.this.listeners.remove(event);
            if (listener != null) {
              listener.close();
            }
          }
        }
      }
    }
  }

}
