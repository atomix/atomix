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

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.internal.ResourceCommand;
import io.atomix.resource.internal.ResourceEvent;
import io.atomix.resource.internal.ResourceQuery;

import java.util.*;

/**
 * Base class for resource state machines.
 * <p>
 * Resource implementations should extend the resource state machine for support in Atomix core.
 * This state machine implements functionality necessary to multiplexing the state machine on a
 * single Copycat log and provides facilities for configuring and cleaning up the resource upon
 * deletion.
 * <p>
 * To implement a resource state machine, simply extend this class. State machines must adhere
 * to the same rules as those outlined in the Copycat documentation. See the {@link StateMachine}
 * documentation for more information.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends ResourceStateMachine {
 *     private final Map<Object, Object> map = new HashMap<>();
 *
 *     public void put(Commit<PutCommand> commit) {
 *       try {
 *         map.put(commit.operation().key(), commit.operation().value());
 *       } finally {
 *         commit.close();
 *       }
 *     }
 *   }
 *   }
 * </pre>
 * Resource state machines should override the {@link #delete()} method to perform cleanup of
 * the resource state upon deletion of the resource. Cleanup tasks might include notifying clients
 * of the deletion of the resource or releasing {@link Commit commits} held by the state machine.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends ResourceStateMachine {
 *     private final Map<Object, Commit<PutCommand>> map = new HashMap<>();
 *
 *     public void put(Commit<PutCommand> commit) {
 *       map.put(commit.operation().key(), commit);
 *     }
 *
 *     public void delete() {
 *       for (Map.Entry<Object, Commit<PutCommand>> entry : map.entrySet()) {
 *         entry.getValue().close();
 *       }
 *       map.clear();
 *     }
 *   }
 *   }
 * </pre>
 * Resource state machines implement Copycat's {@link SessionListener} interface to allow state
 * machines to listen for changes in client sessions. When used in an Atomix cluster, sessions
 * are specific to a state machine and not the entire cluster. When a client or replica opens
 * a resource, a new logical session to that resource is opened and the session listener's
 * {@link SessionListener#register(ServerSession)} method will be called. When a client or replica
 * closes a resource, the {@link SessionListener#close(ServerSession)} method will be called.
 * In other words, sessions in resource state machines represent a client's open resource instance,
 * and event messages {@link ServerSession#publish(String, Object) published} to that session will
 * be received by the specific client side {@link Resource} instance.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends ResourceStateMachine {
 *     private final Map<Object, Object> map = new HashMap<>();
 *     private final Set<ServerSession> listeners = new HashSet<>();
 *
 *     public void listen(Commit<ListenCommand> commit) {
 *       listeners.add(commit.session());
 *     }
 *
 *     public void put(Commit<PutCommand> commit) {
 *       map.put(commit.operation().key(), commit.operation().value());
 *       for (ServerSession listener : listeners) {
 *         listener.publish(commit.operation().key(), commit.operation().value());
 *       }
 *     }
 *
 *     public void close(ServerSession session) {
 *       listeners.remove(session);
 *     }
 *   }
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ResourceStateMachine extends StateMachine implements SessionListener {
  protected final Properties config;
  private final Map<Integer, Set<ServerSession>> eventListeners = new HashMap<>();

  protected ResourceStateMachine(Properties config) {
    this.config = Assert.notNull(config, "config");
  }

  @Override
  public final void init(StateMachineExecutor executor) {
    executor.serializer().register(ResourceCommand.class, -50);
    executor.serializer().register(ResourceQuery.class, -51);
    executor.serializer().register(ResourceQuery.Config.class, -52);
    executor.serializer().register(ResourceCommand.Delete.class, -53);
    executor.serializer().register(ResourceEvent.class, -49);

    executor.context().sessions().addListener(this);

    ResourceStateMachineExecutor wrappedExecutor = new ResourceStateMachineExecutor(executor);
    wrappedExecutor.register(ResourceQuery.Config.class, this::config);
    wrappedExecutor.<ResourceCommand.Register>register(ResourceCommand.Register.class, this::register);
    wrappedExecutor.<ResourceCommand.Unregister>register(ResourceCommand.Unregister.class, this::unregister);
    wrappedExecutor.<ResourceCommand.Delete>register(ResourceCommand.Delete.class, this::delete);
    super.init(wrappedExecutor);
  }

  @Override
  public void register(ServerSession session) {
  }

  @Override
  public void unregister(ServerSession session) {
  }

  @Override
  public void expire(ServerSession session) {
  }

  @Override
  public void close(ServerSession session) {
  }

  /**
   * Notifies all subscribed clients of an event.
   *
   * @param event The event message.
   */
  protected void notify(Resource.Event event) {
    Set<ServerSession> sessions = eventListeners.get(event.type().id());
    if (sessions != null) {
      for (ServerSession session : sessions) {
        session.publish("event", new ResourceEvent(event.type().id(), event));
      }
    }
  }

  /**
   * Returns the resource configuration.
   */
  private Properties config(Commit<ResourceQuery.Config> commit) {
    try {
      return config;
    } finally {
      commit.close();
    }
  }

  /**
   * Registers an event listener.
   */
  private void register(Commit<ResourceCommand.Register> commit) {
    Set<ServerSession> sessions = eventListeners.computeIfAbsent(commit.command().event(), k -> new HashSet<>());
    sessions.add(commit.session());
  }

  /**
   * Unregisters an event listener.
   */
  private void unregister(Commit<ResourceCommand.Unregister> commit) {
    Set<ServerSession> sessions = eventListeners.computeIfAbsent(commit.command().event(), k -> new HashSet<>());
    sessions.remove(commit.session());
    if (sessions.isEmpty()) {
      eventListeners.remove(commit.command().event());
    }
  }


  /**
   * Handles a delete command.
   */
  private void delete(Commit<ResourceCommand.Delete> commit) {
    try {
      delete();
    } finally {
      commit.close();
    }
  }

  /**
   * Cleans up deleted state machine state.
   * <p>
   * This method is called when a client {@link Resource#delete() deletes} a resource in the cluster.
   * This method is guaranteed to eventually be called on all servers at the same point in logical time
   * (i.e. {@code index}). State machines should override this method to clean up state machine state,
   * including releasing {@link Commit}s held by the state machine.
   * <pre>
   *   {@code
   *   public class MapStateMachine extends ResourceStateMachine {
   *     private final Map<Object, Commit<PutCommand>> map = new HashMap<>();
   *
   *     public void put(Commit<PutCommand> commit) {
   *       map.put(commit.operation().key(), commit);
   *     }
   *
   *     public void delete() {
   *       for (Map.Entry<Object, Commit<PutCommand>> entry : map.entrySet()) {
   *         entry.getValue().close();
   *       }
   *       map.clear();
   *     }
   *   }
   *   }
   * </pre>
   * Failing to override this method to clean up commits held by a resource state machine is considered
   * a bug and will eventually result in disk filling up.
   */
  public void delete() {
  }

}
