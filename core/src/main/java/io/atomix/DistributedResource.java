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
package io.atomix;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.StateMachine;
import io.atomix.resource.ResourceContext;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for fault-tolerant stateful distributed objects.
 * <p>
 * Resources are stateful distributed objects that can be accessed by any {@link Atomix} instance
 * in a cluster. Each resource in a cluster is associated with a {@code key} with which clients can
 * reference the resource. For each distributed resource, one or many instances (clients) of the
 * resource can be created on any node in the cluster.
 * <p>
 * Resources are created via the {@link Atomix} resource manager. When a resource is created, a
 * replicated state machine is created for the resource on every {@link AtomixServer} or
 * {@link AtomixReplica} in the cluster. Operations on the resource are translated into
 * {@link Command}s and {@link Query}s which are submitted to the cluster where they're logged and
 * replicated.
 * <p>
 * Resources have varying consistency guarantees depending on the configured resource {@link Consistency}
 * and the semantics of the specific resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class DistributedResource<T extends DistributedResource<T>> {
  protected ResourceContext context;
  private Consistency consistency = Consistency.ATOMIC;

  /**
   * Returns the resource instance ID.
   * <p>
   * The instance ID is guaranteed to be unique to this single instance of the resource. No other
   * instance of any resource in the cluster will have the same instance ID.
   *
   * @return The resource instance ID.
   */
  public long id() {
    return context.id();
  }

  /**
   * Returns the key with which this resource instance is associated.
   * <p>
   * The resource key is unique to the set of resource instances that access the same key in the
   * {@link Atomix} cluster.
   *
   * @return The resource key.
   */
  public String key() {
    return context.key();
  }

  /**
   * Returns the resource thread context.
   *
   * @return The resource thread context.
   */
  public ThreadContext context() {
    return context.context();
  }

  /**
   * Initializes the resource.
   *
   * @param context The resource context.
   * @throws NullPointerException if {@code context} is null
   */
  protected void open(ResourceContext context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Sets the resource consistency level.
   * <p>
   * The configured consistency level specifies how operations on the resource should be handled by the
   * cluster. Consistency levels dictate the order in which reads, writes, and events should be handled
   * by the Atomix cluster and the consistency requirements for completing different types of operations.
   * <p>
   * Note that consistency configurations apply only to a single instance of a distributed resource. Two
   * instances of the same resource on the same or different nodes can have differen consistency requirements,
   * and the cluster will obey those differences.
   * <p>
   * By default, all resource operations are submitted to the cluster with the {@link Consistency#ATOMIC}
   * consistency level. Atomic consistency means that the distributed resource will behave as a single
   * object for all instances. Users can decrease the default consistency level, but note that in some
   * cases resource implementations may override the configured {@link Consistency} for safety. For instance,
   * a leader election may enforce atomic consistency at all times to ensure no two leaders can be
   * elected at the same time.
   *
   * @param consistency The resource consistency level.
   * @return The resource instance.
   */
  @SuppressWarnings("unchecked")
  public T with(Consistency consistency) {
    this.consistency = Assert.notNull(consistency, "consistency");
    return (T) this;
  }

  /**
   * Returns the resource state machine class.
   * <p>
   * For custom distributed resources, when a resource is {@link Atomix#create(String, Class) created},
   * an instance of the returned {@link StateMachine} class will be created on each server in the cluster.
   * The resource state machine is responsible for managing the state machine's state. See the
   * state machine documentation for information on how to implement state machines.
   *
   * @see StateMachine
   *
   * @return The resource state machine class.
   */
  protected abstract Class<? extends StateMachine> stateMachine();

  /**
   * Submits a write operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the configured {@link Consistency#writeConsistency()} if
   * it does not explicitly override {@link Command#consistency()} to provide a static consistency level.
   *
   * @param command The command to submit.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   * @throws NullPointerException if {@code command} is null
   */
  protected <R> CompletableFuture<R> submit(Command<R> command) {
    return context.submit(Assert.notNull(command, "command"), consistency);
  }

  /**
   * Submits a read operation for this resource to the cluster.
   * <p>
   * The read operation will be submitted with the configured {@link Consistency#readConsistency()} if
   * it does not explicitly override {@link Query#consistency()} to provide a static consistency level.
   *
   * @param query The query to submit.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   * @throws NullPointerException if {@code query} is null
   */
  protected <R> CompletableFuture<R> submit(Query<R> query) {
    return context.submit(Assert.notNull(query, "query"), consistency);
  }

  /**
   * Deletes the resource.
   *
   * @return A completable future to be called once the resource has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return context.delete();
  }

  @Override
  public int hashCode() {
    return 37 * 23 + (int)(id() ^ (id() >>> 32));
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isAssignableFrom(object.getClass()) && ((DistributedResource<?>) object).id() == id();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id());
  }

}
