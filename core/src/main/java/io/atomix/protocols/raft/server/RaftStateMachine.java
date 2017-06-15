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
package io.atomix.protocols.raft.server;

import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.error.CommandException;
import io.atomix.protocols.raft.server.session.SessionListener;
import io.atomix.protocols.raft.server.session.Sessions;
import io.atomix.util.serializer.Serializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.time.Clock;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link RaftServer}.
 * <p>
 * State machines are responsible for handling {@link RaftOperation operations} submitted to the Raft cluster and
 * filtering {@link RaftCommit committed} operations out of the Raft log. The most important rule of state machines is
 * that <em>state machines must be deterministic</em> in order to maintain Copycat's consistency guarantees. That is,
 * state machines must not change their behavior based on external influences and have no side effects. Users should
 * <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link io.atomix.protocols.raft.RaftCommand commands} and {@link io.atomix.protocols.raft.RaftQuery queries}
 * (i.e. <em>operations</em>) are submitted to the Raft cluster, the {@link RaftServer} will log and replicate them as
 * necessary and, once complete, apply them to the configured state machine.
 * <p>
 * <h3>State machine operations</h3>
 * State machine operations are implemented as methods on the state machine. Operations can be automatically detected
 * by the state machine during setup or can be explicitly registered by overriding the {@link #configure(StateMachineExecutor)}
 * method. Each operation method must take a single {@link RaftCommit} argument for a specific operation type.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends StateMachine {
 *
 *     public Object put(Commit<Put> commit) {
 *       Commit<Put> previous = map.put(commit.operation().key(), commit);
 *       if (previous != null) {
 *         try {
 *           return previous.operation().value();
 *         } finally {
 *           previous.close();
 *         }
 *       }
 *       return null;
 *     }
 *
 *     public Object get(Commit<Get> commit) {
 *       try {
 *         Commit<Put> current = map.get(commit.operation().key());
 *         return current != null ? current.operation().value() : null;
 *       } finally {
 *         commit.close();
 *       }
 *     }
 *   }
 *   }
 * </pre>
 * When operations are applied to the state machine they're wrapped in a {@link RaftCommit} object. The commit provides the
 * context of how the command or query was committed to the cluster, including the log {@link RaftCommit#index()}, the
 * {@link io.atomix.protocols.raft.server.session.ServerSession} from which the operation was submitted, and the approximate
 * wall-clock {@link RaftCommit#time()} at which the commit was written to the Raft log. Note that the commit time is
 * guaranteed to progress monotonically, but it may not be representative of the progress of actual time. See the
 * {@link RaftCommit} documentation for more information.
 * <p>
 * State machine operations are guaranteed to be executed in the order in which they were submitted by the client,
 * always in the same thread, and thus always sequentially. State machines do not need to be thread safe, but they must
 * be deterministic. That is, state machines are guaranteed to see {@link io.atomix.protocols.raft.RaftCommand}s in the
 * same order on all servers, and given the same commands in the same order, all servers' state machines should arrive at
 * the same state with the same output (return value). The return value of each operation callback is the response value
 * that will be sent back to the client.
 * <p>
 * <h3>Deterministic scheduling</h3>
 * The {@link StateMachineExecutor} is responsible for executing state machine operations sequentially and provides an
 * interface similar to that of {@link java.util.concurrent.ScheduledExecutorService} to allow state machines to schedule
 * time-based callbacks. Because of the determinism requirement, scheduled callbacks are guaranteed to be executed
 * deterministically as well. The executor can be accessed via the {@link #executor} field.
 * See the {@link StateMachineExecutor} documentation for more information.
 * <pre>
 *   {@code
 *   public void putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).close();
 *     });
 *   }
 *   }
 * </pre>
 * <p>
 * During command or scheduled callbacks, {@link Sessions} can be used to send state machine events back to the client.
 * For instance, a lock state machine might use a client's {@link io.atomix.protocols.raft.server.session.ServerSession}
 * to send a lock event to the client.
 * <pre>
 *   {@code
 *   public void unlock(Commit<Unlock> commit) {
 *     try {
 *       Commit<Lock> next = queue.poll();
 *       if (next != null) {
 *         next.session().publish("lock");
 *       }
 *     } finally {
 *       commit.close();
 *     }
 *   }
 *   }
 * </pre>
 * Attempts to {@link io.atomix.protocols.raft.server.session.ServerSession#publish(Object) publish}
 * events during the execution will result in an {@link IllegalStateException}.
 * <p>
 * Even though state machines on multiple servers may appear to publish the same event, Copycat's protocol ensures that only
 * one server ever actually sends the event. Still, it's critical that all state machines publish all events to ensure
 * consistency and fault tolerance. In the event that a server fails after publishing a session event, the client will transparently
 * reconnect to another server and retrieve lost event messages.
 * <p>
 * <h3>Snapshotting</h3>
 * On top of Copycat's log cleaning algorithm mentioned above, Copycat provides a mechanism for storing and loading
 * snapshots of a state machine's state. Snapshots are images of the state machine's state stored at a specific
 * point in logical time (an {@code index}). To support snapshotting, state machine implementations should implement
 * the {@link Snapshottable} interface.
 * <pre>
 *   {@code
 *   public class ValueStateMachine extends StateMachine implements Snapshottable {
 *     private Object value;
 *
 *     public void set(Commit<SetValue> commit) {
 *       this.value = commit.operation().value();
 *       commit.close();
 *     }
 *
 *     public void snapshot(SnapshotWriter writer) {
 *       writer.writeObject(value);
 *     }
 *   }
 *   }
 * </pre>
 * For snapshottable state machines, Copycat will periodically request a {@link io.atomix.protocols.raft.storage.snapshot.Snapshot Snapshot}
 * of the state machine's state by calling the {@link Snapshottable#snapshot(io.atomix.protocols.raft.storage.snapshot.SnapshotWriter)}
 * method. Once the state machine has written a snapshot of its state, Copycat will automatically remove all commands
 * associated with the state machine from the underlying log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 * @see RaftCommit
 * @see StateMachineContext
 * @see StateMachineExecutor
 */
public abstract class RaftStateMachine implements Snapshottable {
  protected final Serializer serializer;
  protected StateMachineExecutor executor;
  protected StateMachineContext context;
  protected Clock clock;
  protected Sessions sessions;

  protected RaftStateMachine(Serializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns the state machine serializer.
   *
   * @return The state machine serializer.
   */
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Initializes the state machine.
   *
   * @param executor The state machine executor.
   * @throws NullPointerException if {@code context} is null
   */
  public void init(StateMachineExecutor executor) {
    this.executor = checkNotNull(executor, "executor cannot be null");
    this.context = executor.context();
    this.clock = context.clock();
    this.sessions = context.sessions();
    if (this instanceof SessionListener) {
      executor.context().sessions().addListener((SessionListener) this);
    }
    configure(executor);
  }

  /**
   * Configures the state machine.
   * <p>
   * By default, this method will configure state machine operations by extracting public methods with
   * a single {@link RaftCommit} parameter via reflection. Override this method to explicitly register
   * state machine operations via the provided {@link StateMachineExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected void configure(StateMachineExecutor executor) {
    registerOperations();
  }

  /**
   * Closes the state machine.
   */
  public void close() {

  }

  /**
   * Registers operations for the class.
   */
  private void registerOperations() {
    Class<?> type = getClass();
    for (Method method : type.getMethods()) {
      if (isOperationMethod(method)) {
        registerMethod(method);
      }
    }
  }

  /**
   * Returns a boolean value indicating whether the given method is an operation method.
   */
  private boolean isOperationMethod(Method method) {
    Class<?>[] paramTypes = method.getParameterTypes();
    return paramTypes.length == 1 && paramTypes[0] == RaftCommit.class;
  }

  /**
   * Registers an operation for the given method.
   */
  private void registerMethod(Method method) {
    Type genericType = method.getGenericParameterTypes()[0];
    Class<?> argumentType = resolveArgument(genericType);
    if (argumentType != null && RaftOperation.class.isAssignableFrom(argumentType)) {
      registerMethod(argumentType, method);
    }
  }

  /**
   * Resolves the generic argument for the given type.
   */
  private Class<?> resolveArgument(Type type) {
    if (type instanceof ParameterizedType) {
      ParameterizedType paramType = (ParameterizedType) type;
      return resolveClass(paramType.getActualTypeArguments()[0]);
    } else if (type instanceof TypeVariable) {
      return resolveClass(type);
    } else if (type instanceof Class) {
      TypeVariable<?>[] typeParams = ((Class<?>) type).getTypeParameters();
      return resolveClass(typeParams[0]);
    }
    return null;
  }

  /**
   * Resolves the generic class for the given type.
   */
  private Class<?> resolveClass(Type type) {
    if (type instanceof Class) {
      return (Class<?>) type;
    } else if (type instanceof ParameterizedType) {
      return resolveClass(((ParameterizedType) type).getRawType());
    } else if (type instanceof WildcardType) {
      Type[] bounds = ((WildcardType) type).getUpperBounds();
      if (bounds.length > 0) {
        return (Class<?>) bounds[0];
      }
    }
    return null;
  }

  /**
   * Registers the given method for the given operation type.
   */
  private void registerMethod(Class<?> type, Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType == void.class || returnType == Void.class) {
      registerVoidMethod(type, method);
    } else {
      registerValueMethod(type, method);
    }
  }

  /**
   * Registers an operation with a void return value.
   */
  @SuppressWarnings("unchecked")
  private void registerVoidMethod(Class type, Method method) {
    executor.register(type, wrapVoidMethod(method));
  }

  /**
   * Wraps a void method.
   */
  private Consumer wrapVoidMethod(Method method) {
    return c -> {
      try {
        method.invoke(this, c);
      } catch (InvocationTargetException e) {
        throw new CommandException(e);
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
    };
  }

  /**
   * Registers an operation with a non-void return value.
   */
  @SuppressWarnings("unchecked")
  private void registerValueMethod(Class type, Method method) {
    executor.register(type, wrapValueMethod(method));
  }

  /**
   * Wraps a value method.
   */
  private Function wrapValueMethod(Method method) {
    return c -> {
      try {
        return method.invoke(this, c);
      } catch (InvocationTargetException e) {
        throw new CommandException(e);
      } catch (IllegalAccessException e) {
        throw new AssertionError(e);
      }
    };
  }

}
