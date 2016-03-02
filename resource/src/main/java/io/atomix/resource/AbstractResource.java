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

import io.atomix.catalyst.serializer.SerializerRegistry;
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
    this.type = new ResourceType(getClass());
    this.client = Assert.notNull(client, "client");

    client.serializer().register(ResourceCommand.class, -50);
    client.serializer().register(ResourceQuery.class, -51);
    client.serializer().register(ResourceCommand.Config.class, -52);
    client.serializer().register(ResourceCommand.Delete.class, -53);
    client.serializer().register(ResourceType.class, -54);

    registerTypes(client.serializer().registry());

    this.config = new Config();
    this.options = new Options(Assert.notNull(options, "options"));
    client.onStateChange(this::onStateChange);
  }

  /**
   * Registers serializable types on the given serializer.
   */
  protected void registerTypes(SerializerRegistry registry) {
  }

  /**
   * Called when a client state change occurs.
   */
  private void onStateChange(CopycatClient.State state) {
    this.state = State.valueOf(state.name());
    changeListeners.forEach(l -> l.accept(this.state));
  }

  /**
   * Returns the resource type.
   *
   * @return The resource type.
   */
  public ResourceType type() {
    return type;
  }

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  public Config config() {
    return config;
  }

  /**
   * Returns the resource options.
   *
   * @return The configured resource options.
   */
  public Options options() {
    return options;
  }

  /**
   * Returns the current resource state.
   * <p>
   * The resource's {@link State} is indicative of the resource's ability to communicate with the cluster at any given
   * time. Users of the resource should use the state to determine when guarantees may be lost. See the {@link State}
   * documentation for information on the specific states, and see resource implementation documentation for the
   * implications of different states on resource consistency.
   *
   * @return The current resource state.
   */
  public State state() {
    return state;
  }

  /**
   * Registers a resource state change listener.
   *
   * @param callback The callback to call when the resource state changes.
   * @return The state change listener.
   */
  public Listener<State> onStateChange(Consumer<State> callback) {
    return new StateChangeListener(Assert.notNull(callback, "callback"));
  }

  /**
   * Returns the resource thread context.
   *
   * @return The resource thread context.
   */
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Returns the configured write consistency level.
   * <p>
   * Resource consistency levels are configured via the {@link #with(WriteConsistency)} setter. By default,
   * all resources submit commands under the {@link WriteConsistency#ATOMIC} consistency level. See the
   * {@link WriteConsistency} documentation for information about guarantees provided by specific consistency
   * levels.
   *
   * @return The configured resource consistency level.
   */
  public WriteConsistency writeConsistency() {
    return writeConsistency;
  }

  /**
   * Sets the write consistency level.
   * <p>
   * The configured consistency level specifies how operations on the resource should be handled by the
   * cluster. Consistency levels dictate the order in which reads, writes, and events should be handled
   * by the Atomix cluster and the consistency requirements for completing different types of operations.
   * <p>
   * Note that consistency configurations apply only to a single instance of a distributed resource. Two
   * instances of the same resource on the same or different nodes can have different consistency requirements,
   * and the cluster will obey those differences.
   * <p>
   * By default, all resource operations are submitted to the cluster with the {@link WriteConsistency#ATOMIC}
   * consistency level. Atomic consistency means that the distributed resource will behave as a single
   * object for all instances. Users can decrease the default consistency level, but note that in some
   * cases resource implementations may override the configured {@link WriteConsistency} for safety. For instance,
   * a leader election may enforce atomic consistency at all times to ensure no two leaders can be
   * elected at the same time.
   *
   * @param consistency The write consistency level.
   * @return The resource instance.
   * @throws NullPointerException if {@code consistency} is null
   */
  @SuppressWarnings("unchecked")
  public T with(WriteConsistency consistency) {
    this.writeConsistency = Assert.notNull(consistency, "consistency");
    return (T) this;
  }

  /**
   * Returns the configured read consistency level.
   * <p>
   * Resource consistency levels are configured via the {@link #with(ReadConsistency)} setter. By default,
   * all resources submit commands under the {@link ReadConsistency#ATOMIC} consistency level. See the
   * {@link ReadConsistency} documentation for information about guarantees provided by specific consistency
   * levels.
   *
   * @return The configured resource consistency level.
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  /**
   * Sets the read consistency level.
   * <p>
   * The configured consistency level specifies how operations on the resource should be handled by the
   * cluster. Consistency levels dictate the order in which reads, writes, and events should be handled
   * by the Atomix cluster and the consistency requirements for completing different types of operations.
   * <p>
   * Note that consistency configurations apply only to a single instance of a distributed resource. Two
   * instances of the same resource on the same or different nodes can have different consistency requirements,
   * and the cluster will obey those differences.
   * <p>
   * By default, all resource operations are submitted to the cluster with the {@link WriteConsistency#ATOMIC}
   * consistency level. Atomic consistency means that the distributed resource will behave as a single
   * object for all instances. Users can decrease the default consistency level, but note that in some
   * cases resource implementations may override the configured {@link WriteConsistency} for safety. For instance,
   * a leader election may enforce atomic consistency at all times to ensure no two leaders can be
   * elected at the same time.
   *
   * @param consistency The read consistency level.
   * @return The resource instance.
   * @throws NullPointerException if {@code consistency} is null
   */
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

  /**
   * Opens the resource.
   * <p>
   * Once the resource is opened, the resource will be transitioned to the {@link State#CONNECTED} state
   * and the returned {@link CompletableFuture} will be completed.
   *
   * @return A completable future to be completed once the resource is opened.
   */
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

  /**
   * Closes the resource.
   * <p>
   * Once the resource is closed, the resource will be transitioned to the {@link State#CLOSED} state and
   * the returned {@link CompletableFuture} will be completed. Thereafter, attempts to operate on the resource
   * will fail.
   *
   * @return A completable future to be completed once the resource is closed.
   */
  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /**
   * Deletes the resource state.
   *
   * @return A completable future to be completed once the resource has been deleted.
   */
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
