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
package net.kuujo.copycat.manager;

import net.kuujo.catalog.client.session.Session;
import net.kuujo.catalyst.util.Assert;
import net.kuujo.catalyst.util.Listener;
import net.kuujo.catalyst.util.Listeners;
import net.kuujo.copycat.resource.ResourceEvent;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ManagedResourceSession implements Session {

  /**
   * Resource session state.
   */
  private enum State {
    OPEN,
    CLOSED,
    EXPIRED
  }

  private State state = State.OPEN;
  private final long resource;
  private final Session parent;
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();
  private final Listeners<?> receiveListeners = new Listeners();

  public ManagedResourceSession(long resource, Session parent) {
    this.resource = resource;
    this.parent = parent;
    parent.onOpen(this::handleOpen);
    parent.onClose(this::handleClose);
    parent.onEvent(this::handleEvent);
  }

  @Override
  public long id() {
    return parent.id();
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    return parent.publish(new ResourceEvent<>(resource, Assert.notNull(message, "message")));
  }

  /**
   * handles receiving an event.
   */
  @SuppressWarnings("unchecked")
  private void handleEvent(ResourceEvent event) {
    if (event.resource() == resource) {
      for (Consumer listener : receiveListeners) {
        listener.accept(event);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Listener onEvent(Consumer<T> listener) {
    return receiveListeners.add((Consumer) Assert.notNull(listener, "listener"));
  }

  @Override
  public boolean isOpen() {
    return state == State.OPEN;
  }

  /**
   * Handles a session open event.
   */
  private void handleOpen(Session session) {
    state = State.OPEN;
    for (Consumer<Session> listener : openListeners) {
      listener.accept(this);
    }
  }

  @Override
  public Listener<Session> onOpen(Consumer<Session> listener) {
    return openListeners.add(Assert.notNull(listener, "listener"));
  }

  /**
   * Handles a session close event.
   */
  private void handleClose(Session session) {
    state = session.isExpired() ? State.EXPIRED : State.CLOSED;
    for (Consumer<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public Listener<Session> onClose(Consumer<Session> listener) {
    return closeListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED || state == State.EXPIRED;
  }

  @Override
  public boolean isExpired() {
    return state == State.EXPIRED;
  }

}
