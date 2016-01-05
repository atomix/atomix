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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.session.Session;
import io.atomix.manager.state.CloseResource;
import io.atomix.manager.state.CreateResource;
import io.atomix.manager.state.DeleteResource;
import io.atomix.manager.state.GetResource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class InstanceClient implements CopycatClient {
  private volatile long resource;
  private final Instance instance;
  private final CopycatClient client;
  private volatile Session clientSession;
  private volatile InstanceSession session;
  private volatile State state;
  private final Listener<State> changeListener;
  private final Map<String, Set<EventListener>> eventListeners = new ConcurrentHashMap<>();
  private final Map<String, Listener<InstanceEvent<?>>> listeners = new ConcurrentHashMap<>();
  private final Set<StateChangeListener> changeListeners = new CopyOnWriteArraySet<>();
  private volatile CompletableFuture<CopycatClient> openFuture;
  private volatile CompletableFuture<CopycatClient> recoverFuture;
  private volatile CompletableFuture<Void> closeFuture;

  public InstanceClient(Instance instance, CopycatClient client) {
    this.instance = Assert.notNull(instance, "instance");
    this.client = Assert.notNull(client, "client");
    this.state = State.CLOSED;
    this.changeListener = client.onStateChange(this::onStateChange);
  }

  @Override
  public State state() {
    return state;
  }

  /**
   * Called when the parent client's state changes.
   */
  private void onStateChange(State state) {
    // Don't allow the underlying client to transition the instance's state if the state is CLOSED.
    if (this.state != State.CLOSED && this.state != state) {
      // If the underlying client registered a new session, set the instance's state to SUSPENDED
      // and recover the instance using the new session.
      if (!client.session().equals(clientSession)) {
        clientSession = client.session();
        if (this.state != State.SUSPENDED) {
          this.state = State.SUSPENDED;
          changeListeners.forEach(l -> l.accept(state));
        }
        recover();
      }
      // If the underlying client's state has changed, update the instance's state and invoke listeners.
      // This ensures that the instance's state changes to SUSPENDED when the client's state changes
      // to SUSPENDED, and the instance's state only changes back to CONNECTED if the underlying client's
      // state changed back to CONNECTED *and* its session was maintained.
      else {
        this.state = state;
        changeListeners.forEach(l -> l.accept(state));
      }
    }
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return client.onStateChange(callback);
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public Transport transport() {
    return client.transport();
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public Serializer serializer() {
    return client.serializer();
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (command instanceof ResourceStateMachine.DeleteCommand) {
      return client.submit(new InstanceCommand<>(resource, command)).thenCompose(v -> client.submit(new DeleteResource(resource))).thenApply(result -> null);
    }
    return client.submit(new InstanceCommand<>(resource, command));
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return client.submit(new InstanceQuery<>(resource, query));
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
      this.listeners.put(event, client.onEvent(event, message -> handleEvent(event, message)));
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
  public synchronized CompletableFuture<CopycatClient> open() {
    if (state != State.CLOSED)
      return Futures.exceptionalFuture(new IllegalStateException("client already open"));

    if (openFuture == null) {
      switch (instance.method()) {
        case GET:
          openFuture = openGet();
          break;
        case CREATE:
          openFuture = openCreate();
          break;
      }
    }
    return openFuture;
  }

  /**
   * Opens the resource using the GET method.
   */
  private CompletableFuture<CopycatClient> openGet() {
    return client.submit(new GetResource(instance.key(), instance.type().id())).thenApply(this::completeOpen);
  }

  /**
   * Opens the resource using the CREATE method.
   */
  private CompletableFuture<CopycatClient> openCreate() {
    return client.submit(new CreateResource(instance.key(), instance.type().id())).thenApply(this::completeOpen);
  }

  /**
   * Completes the registration of a new session.
   */
  private synchronized CopycatClient completeOpen(long resourceId) {
    this.resource = resourceId;
    this.clientSession = client.session();
    this.session = new InstanceSession(resourceId, clientSession, client.context());
    this.state = State.CONNECTED;
    changeListeners.forEach(l -> l.accept(State.CONNECTED));
    openFuture = null;
    recoverFuture = null;
    return this;
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public synchronized CompletableFuture<CopycatClient> recover() {
    if (state != State.SUSPENDED)
      return Futures.exceptionalFuture(new IllegalStateException("client not suspended"));

    if (recoverFuture == null) {
      switch (instance.method()) {
        case GET:
          recoverFuture = openGet();
          break;
        case CREATE:
          recoverFuture = openCreate();
          break;
      }
    }
    return recoverFuture;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (state == State.CLOSED)
      return Futures.exceptionalFuture(new IllegalStateException("client already closed"));

    if (closeFuture == null) {
      closeFuture = client.submit(new CloseResource(resource))
        .whenComplete((result, error) -> {
          synchronized (this) {
            instance.close();
            changeListener.close();
            for (Map.Entry<String, Listener<InstanceEvent<?>>> entry : listeners.entrySet()) {
              entry.getValue().close();
            }
            listeners.clear();
            this.state = State.CLOSED;
            changeListeners.forEach(l -> l.accept(State.CLOSED));
            closeFuture = null;
          }
        });
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d]", getClass().getSimpleName(), resource);
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
      synchronized (InstanceClient.this) {
        Set<EventListener> listeners = eventListeners.get(event);
        if (listeners != null) {
          listeners.remove(this);
          if (listeners.isEmpty()) {
            eventListeners.remove(event);
            Listener listener = InstanceClient.this.listeners.remove(event);
            if (listener != null) {
              listener.close();
            }
          }
        }
      }
    }
  }

  /**
   * Resource state change listener.
   */
  private class StateChangeListener implements Listener<State> {
    private final Consumer<State> callback;

    private StateChangeListener(Consumer<State> callback) {
      this.callback = callback;
      changeListeners.add(this);
    }

    @Override
    public void accept(State state) {
      callback.accept(state);
    }

    @Override
    public void close() {
      changeListeners.remove(this);
    }
  }

}
