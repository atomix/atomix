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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.session.Session;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Base class for fault-tolerant stateful distributed objects.
 * <p>
 * Resources are stateful distributed objects that run across a set of {@link io.atomix.copycat.server.CopycatServer}s
 * in a cluster.
 * <p>
 * Resources can be created either as standalone {@link io.atomix.copycat.server.StateMachine}s in
 * a typical Copycat cluster or as {@code Atomix} resources. Operations on the resource are translated into
 * {@link Command}s and {@link Query}s which are submitted to the cluster where they're logged and
 * replicated.
 * <p>
 * Resource implementations must be annotated with the {@link ResourceTypeInfo} annotation to indicate the
 * {@link ResourceStateMachine} to be used by the Atomix resource manager. Additionally, resources must be
 * registered via the {@code ServiceLoader} pattern in a file at {@code META-INF/services/io.atomix.resource.Resource}
 * on the class path. The resource registration allows the Atomix resource manager to locate and load the resource
 * state machine on each server in the cluster.
 *
 * @param <T> resource type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Resource<T extends Resource<T>> extends Managed<T> {

  /**
   * Base class for cluster-wide resource configurations.
   * <p>
   * Resource configurations control options specific to the resource's replicated {@link ResourceStateMachine}.
   * These options might include a maximum collection size or the order of values in a multi-map.
   */
  class Config extends Properties implements Serializable {
    public Config() {
    }

    public Config(Properties defaults) {
      super();
      for (String property : defaults.stringPropertyNames()) {
        setProperty(property, defaults.getProperty(property));
      }
    }
  }

  /**
   * Base class for local resource options.
   * <p>
   * Options are configurations that are specific to a local resource instance. The first time a resource
   * is created by a client, the client may provide {@code Options} specifying the behavior of the resource
   * instance. Those initial options configure the behavior of the resource instance on the local node only.
   */
  class Options extends Properties {
    public Options() {
    }

    public Options(Properties defaults) {
      super(defaults);
    }
  }

  /**
   * Resource event type.
   * <p>
   * An event type should be created for each distinct {@link Event} supported by a resource type.
   * The event type provides an {@link #id()} that is used internally by Atomix to invoke event
   * handlers within the resource client.
   */
  interface EventType {
    /**
     * Returns the event type ID.
     * <p>
     * The event type ID is used to identify event types when published from the server state
     * machine to the client. Event type IDs must be unique within a resource type.
     *
     * @return The event type ID. This must be unique within a resource type.
     */
    int id();
  }

  /**
   * Resource event interface.
   * <p>
   * Resources should implement this interface for event messages sent from {@link ResourceStateMachine}s
   * to resource clients. Each event type should be associated with a unique {@link EventType} that indicates
   * the event identifier.
   */
  interface Event {
    /**
     * Returns the resource event type.
     * <p>
     * The event type should be unique to the specific event as it's used as an event identifier.
     *
     * @return The resource event type.
     */
    EventType type();
  }

  /**
   * Resource session state constants.
   * <p>
   * The resource state is indicative of the resource's ability to communicate with the cluster within the
   * context of the underlying {@link Session}. In some cases, resource state changes may be indicative of a
   * loss of guarantees. Users of the resource should {@link Resource#onStateChange(Consumer) watch} the state
   * of a resource to determine when guarantees are lost and react to changes in the resource's ability to communicate
   * with the cluster.
   * <p>
   * <pre>
   *   {@code
   *   resource.onStateChange(state -> {
   *     switch (state) {
   *       case OPEN:
   *         // The resource is healthy
   *         break;
   *       case SUSPENDED:
   *         // The resource is unhealthy and operations may be unsafe
   *         break;
   *       case CLOSED:
   *         // The resource has been closed and pending operations have failed
   *         break;
   *     }
   *   });
   *   }
   * </pre>
   * So long as the resource is in the {@link #CONNECTED} state, all guarantees with respect to reads and writes will
   * be maintained, and a loss of the {@code CONNECTED} state may indicate a loss of linearizability. See the specific
   * states for more info.
   * <p>
   * Reference resource documentation for implications of the various states on specific resources.
   */
  enum State {

    /**
     * Indicates that the resource is connected and operating normally.
     * <p>
     * The {@code CONNECTED} state indicates that the resource is healthy and operating normally. Operations submitted
     * and completed while the resource is in this state are guaranteed to adhere to the configured consistency level.
     */
    CONNECTED,

    /**
     * Indicates that the resource is suspended and its session may or may not be expired.
     * <p>
     * The {@code SUSPENDED} state is indicative of an inability to communicate with the cluster within the context of
     * the underlying client's {@link Session}. Operations performed on resources in this state should be considered
     * unsafe. An operation performed on a {@link #CONNECTED} resource that transitions to the {@code SUSPENDED} state
     * prior to the operation's completion may be committed multiple times in the event that the underlying session
     * is ultimately {@link Session.State#EXPIRED expired}, thus breaking linearizability. Additionally, state machines
     * may see the session expire while the resource is in this state.
     * <p>
     * A resource that is in the {@code SUSPENDED} state may transition back to {@link #CONNECTED} once its underlying
     * session is recovered. However, operations not yet completed prior to the resource's recovery may lose linearizability
     * guarantees. If an operation is submitted while a resource is in the {@link #CONNECTED} state and the resource loses
     * and recovers its session, the operation may be applied to the resource's state more than once. Operations completed
     * across sessions are guaranteed to be performed at-least-once only.
     */
    SUSPENDED,

    /**
     * Indicates that the resource is closed.
     * <p>
     * A resource may transition to this state as a result of an expired session or an explicit {@link Resource#close() close}
     * by the user or a closure of the resource's underlying client.
     */
    CLOSED

  }

  /**
   * Returns the resource type.
   *
   * @return The resource type.
   */
  ResourceType type();

  /**
   * Returns the resource serializer.
   *
   * @return The resource serializer.
   */
  Serializer serializer();

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  Config config();

  /**
   * Returns the resource options.
   *
   * @return The configured resource options.
   */
  Options options();

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
  State state();

  /**
   * Registers a resource state change listener.
   *
   * @param callback The callback to call when the resource state changes.
   * @return The state change listener.
   */
  Listener<State> onStateChange(Consumer<State> callback);

  /**
   * Registers a listener which gets called after the resource recovery process
   *
   * @param callback The callback to call when the resource finishes recovery process.
   * @return The recovery listener.
   */
  Listener<Integer> onRecovery(Consumer<Integer> callback);

  /**
   * Returns the resource thread context.
   *
   * @return The resource thread context.
   */
  ThreadContext context();

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
  CompletableFuture<T> open();

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
  CompletableFuture<Void> close();

  /**
   * Deletes the resource state.
   *
   * @return A completable future to be completed once the resource has been deleted.
   */
  CompletableFuture<Void> delete();

}
