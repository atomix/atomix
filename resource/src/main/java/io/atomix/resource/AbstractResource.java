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
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.util.ResourceCommand;
import io.atomix.resource.util.ResourceQuery;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
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
  private WriteConsistency writeConsistency = WriteConsistency.ATOMIC;
  private ReadConsistency readConsistency = ReadConsistency.ATOMIC;

  protected AbstractResource(CopycatClient client, Properties options) {
    this(client, null, options);
  }

  protected AbstractResource(CopycatClient client, ResourceType type, Properties options) {
    this.client = Assert.notNull(client, "client");
    if (type == null)
      type = new ResourceType(getClass());
    this.type = type;

    client.serializer().register(ResourceCommand.class, -50);
    client.serializer().register(ResourceQuery.class, -51);
    client.serializer().register(ResourceCommand.Config.class, -52);
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
    this.state = State.valueOf(state.name());
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
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public WriteConsistency writeConsistency() {
    return writeConsistency;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T with(WriteConsistency consistency) {
    this.writeConsistency = Assert.notNull(consistency, "consistency");
    return (T) this;
  }

  @Override
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T with(ReadConsistency consistency) {
    this.readConsistency = Assert.notNull(consistency, "consistency");
    return (T) this;
  }

  /**
   * Submits a write operation for this resource to the cluster.
   * <p>
   * The write operation will be submitted with the configured {@link WriteConsistency#level()} if
   * it does not explicitly override {@link Command#consistency()} to provide a static consistency level.
   *
   * @param command The command to submit.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   * @throws NullPointerException if {@code command} is null
   */
  protected <R> CompletableFuture<R> submit(Command<R> command) {
    return client.submit(new ResourceCommand<>(Assert.notNull(command, "command"), writeConsistency.level()));
  }

  /**
   * Submits a write operation for this resource to the cluster.
   * <p>
   * The write operation will be submitted with the {@link WriteConsistency#level()} if
   * it does not explicitly override {@link Command#consistency()} to provide a static consistency level.
   *
   * @param command The command to submit.
   * @param consistency The consistency with which to submit the command.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   * @throws NullPointerException if {@code command} is null
   */
  protected <R> CompletableFuture<R> submit(Command<R> command, WriteConsistency consistency) {
    return client.submit(new ResourceCommand<>(Assert.notNull(command, "command"), consistency.level()));
  }

  /**
   * Submits a read operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the configured {@link ReadConsistency#level()} if
   * it does not explicitly override {@link Query#consistency()} to provide a static consistency level.
   *
   * @param query The query to submit.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   * @throws NullPointerException if {@code query} is null
   */
  protected <R> CompletableFuture<R> submit(Query<R> query) {
    return client.submit(new ResourceQuery<>(Assert.notNull(query, "query"), readConsistency.level()));
  }

  /**
   * Submits a read operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the {@link ReadConsistency#level()} if
   * it does not explicitly override {@link Query#consistency()} to provide a static consistency level.
   *
   * @param query The query to submit.
   * @param consistency The read consistency level.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   * @throws NullPointerException if {@code query} is null
   */
  protected <R> CompletableFuture<R> submit(Query<R> query, ReadConsistency consistency) {
    return client.submit(new ResourceQuery<>(Assert.notNull(query, "query"), consistency.level()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return client.open()
      .thenCompose(v -> client.submit(new ResourceCommand.Config()))
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
