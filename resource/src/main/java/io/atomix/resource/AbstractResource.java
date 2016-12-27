/*
 * Copyright 2016 the original author or authors.
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
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.internal.ResourceCommand;
import io.atomix.resource.internal.ResourceCopycatClient;
import io.atomix.resource.internal.ResourceQuery;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Abstract resource.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractResource<T extends Resource<T>> implements Resource<T> {
  private final ResourceType type;
  protected final CopycatClient client;
  protected volatile Config config;
  protected final Options options;
  private volatile State state;
  private final Set<StateChangeListener> changeListeners = new CopyOnWriteArraySet<>();
  private final Set<RecoveryListener> recoveryListeners = new CopyOnWriteArraySet<>();
  private final AtomicInteger recoveryAttempt = new AtomicInteger(0);

  protected AbstractResource(CopycatClient client, Properties options) {
    this(client, null, options);
  }

  protected AbstractResource(CopycatClient client, ResourceType type, Properties options) {
    this.client = new ResourceCopycatClient(Assert.notNull(client, "client"));
    if (type == null)
      type = new ResourceType(getClass());
    this.type = type;

    client.serializer().register(ResourceCommand.class, -50);
    client.serializer().register(ResourceQuery.class, -51);
    client.serializer().register(ResourceQuery.Config.class, -52);
    client.serializer().register(ResourceCommand.Delete.class, -53);
    client.serializer().register(ResourceType.class, -54);

    this.config = new Config();
    this.options = new Options(Assert.notNull(options, "options"));
    client.onStateChange(this::onStateChange);
  }

  /**
   * Called when a client state change occurs.
   */
  private void onStateChange(CopycatClient.State state) {
    final State newState = State.valueOf(state.name());
    if (this.state == State.SUSPENDED && newState == State.CONNECTED) {
      final int attempt = recoveryAttempt.incrementAndGet();
      recover(attempt).whenComplete((v, e) -> {
        // @todo what should on error? close client?
        recoveryListeners.forEach(l -> l.accept(attempt));
      });
    }
    this.state = newState;
    changeListeners.forEach(l -> l.accept(this.state));
  }

  @Override
  public ResourceType type() {
    return type;
  }

  @Override
  public Serializer serializer() {
    return client.serializer();
  }

  @Override
  public Config config() {
    return config;
  }

  @Override
  public Options options() {
    return options;
  }

  @Override
  public State state() {
    return state;
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return new StateChangeListener(Assert.notNull(callback, "callback"));
  }

  @Override
  public Listener<Integer> onRecovery(Consumer<Integer> callback) {
    return new RecoveryListener(Assert.notNull(callback, "callback"));
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return client.connect()
      .thenCompose(v -> client.submit(new ResourceQuery.Config()))
      .thenApply(config -> {
        this.config = new Config(config);
        return (T) this;
      });
  }

  @Override
  public boolean isOpen() {
    return state != State.CLOSED;
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.submit(new ResourceCommand.Delete());
  }

  @Override
  public int hashCode() {
    return 37 * 23 + client.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof AbstractResource && ((AbstractResource) object).client.session().id() == client.session().id();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), client.session().id());
  }

  protected CompletableFuture<Void> recover(Integer attempt) {
    return CompletableFuture.completedFuture(null);
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

  /**
   * Resource state change listener.
   */
  private class RecoveryListener implements Listener<Integer> {
    private final Consumer<Integer> callback;

    private RecoveryListener(Consumer<Integer> callback) {
      this.callback = callback;
      recoveryListeners.add(this);
    }

    @Override
    public void accept(Integer attempt) {
      callback.accept(attempt);
    }

    @Override
    public void close() {
      recoveryListeners.remove(this);
    }
  }
}
