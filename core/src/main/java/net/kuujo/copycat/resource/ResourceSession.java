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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceSession implements Session {
  private final long resource;
  private final Session parent;
  private final Context context;
  private final Set<Listener> receiveListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private ListenerContext<ResourceMessage<?>> listener;

  public ResourceSession(long resource, Session parent, Context context) {
    this.resource = resource;
    this.parent = parent;
    this.context = context;
  }

  @Override
  public long id() {
    return parent.id();
  }

  @Override
  public boolean isOpen() {
    return parent.isOpen();
  }

  @Override
  public ListenerContext<Session> onOpen(Listener<Session> listener) {
    return parent.onOpen(listener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> publish(Object message) {
    ResourceMessage resourceMessage = (ResourceMessage) message;
    if (resourceMessage.resource() == resource) {
      return CompletableFuture.runAsync(() -> {
        for (Listener<Object> listener : receiveListeners) {
          listener.accept(resourceMessage.message());
        }
      }, context);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> ListenerContext<T> onReceive(Listener<T> listener) {
    if (receiveListeners.isEmpty()) {
      this.listener = parent.onReceive(this::handleReceive);
    }
    receiveListeners.add(listener);
    return new ReceiveListenerContext<>(listener);
  }

  /**
   * Handles receiving a resource message.
   */
  @SuppressWarnings("unchecked")
  private void handleReceive(ResourceMessage<?> message) {
    if (message.resource() == resource) {
      for (Listener listener : receiveListeners) {
        listener.accept(message.message());
      }
    }
  }

  @Override
  public ListenerContext<Session> onClose(Listener<Session> listener) {
    return parent.onClose(listener);
  }

  @Override
  public boolean isClosed() {
    return parent.isClosed();
  }

  @Override
  public boolean isExpired() {
    return parent.isExpired();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, resource=%d]", getClass().getSimpleName(), id(), resource);
  }

  /**
   * Receive listener context.
   */
  private class ReceiveListenerContext<T> implements ListenerContext<T> {
    private final Listener<T> listener;

    private ReceiveListenerContext(Listener<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      synchronized (ResourceSession.this) {
        receiveListeners.remove(listener);
        if (receiveListeners.isEmpty()) {
          ResourceSession.this.listener.close();
          ResourceSession.this.listener = null;
        }
      }
    }
  }

}
