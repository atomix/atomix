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
package net.kuujo.copycat.raft.server;

import net.kuujo.copycat.raft.ApplicationException;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link RaftServer}.
 * <p>
 * State machines are responsible for handling {@link net.kuujo.copycat.raft.Operation operations} submitted to the Raft
 * cluster and filtering {@link Commit committed} operations out of the Raft log. The most
 * important rule of state machines is that <em>state machines must be deterministic</em> in order to maintain Copycat's
 * consistency guarantees. That is, state machines must not change their behavior based on external influences and have
 * no side effects. Users should <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link net.kuujo.copycat.raft.Command commands} and {@link net.kuujo.copycat.raft.Query queries} are submitted
 * to the Raft cluster, the {@link RaftServer} will log and replicate them as necessary
 * and, once complete, apply them to the configured state machine.
 * <p>
 * State machine commands and queries are defined by annotating public or protected methods with the
 * {@link Apply} annotation:
 * <pre>
 *   {@code
 *   public class SetStateMachine extends StateMachine {
 *     private final Set<String> values = new HashSet<>();
 *
 *     protected boolean applyContains(Commit<ContainsQuery> commit) {
 *       return values.contains(commit.operation().value());
 *     }
 *
 *   }
 *   }
 * </pre>
 * Operations are applied to {@link Apply} annotated methods wrapped in a
 * {@link Commit} object. The {@code Commit} object contains information about how the
 * operation was committed including the {@link Commit#index()} at which it was logged in the Raft log, the
 * {@link Commit#timestamp()} at which it was logged, and the {@link net.kuujo.copycat.raft.Session}
 * that created the commit.
 * <p>
 * Operation methods can return either a synchronous result or an asynchronous result via {@link java.util.concurrent.CompletableFuture}.
 * When the state machine is constructed, the {@link Apply} annotated methods will be
 * evaluated for return types, so asynchronous methods <em>must specify a {@link java.util.concurrent.CompletableFuture}
 * return type</em>. Once an operation is applied to the state machine, the value returned by the operation will be returned
 * to the client that submitted the operation.
 *
 * @see Apply
 * @see Commit
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine implements AutoCloseable {
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Map<Class<? extends Operation>, OperationExecutor> operations = new HashMap<>();
  private OperationExecutor allOperation;
  private long time;

  protected StateMachine() {
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    init(getClass());
  }

  /**
   * Initializes the state machine.
   */
  private void init(Class<?> clazz) {
    while (clazz != null && clazz != Object.class) {
      for (Method method : clazz.getDeclaredMethods()) {
        declareOperations(method);
      }

      for (Class<?> iface : clazz.getInterfaces()) {
        init(iface);
      }
      clazz = clazz.getSuperclass();
    }
  }

  /**
   * Declares any operations defined by the given method.
   */
  private void declareOperations(Method method) {
    Apply apply = method.getAnnotation(Apply.class);
    if (apply != null) {
      method.setAccessible(true);
      for (Class<? extends Operation> operation : apply.value()) {
        if (operation == Apply.All.class) {
          allOperation = new OperationExecutor(method);
        } else if (!operations.containsKey(operation)) {
          operations.put(operation, new OperationExecutor(method));
        }
      }
    }
  }

  /**
   * Finds the operation method for the given operation.
   */
  private OperationExecutor findOperation(Class<? extends Operation> type) {
    OperationExecutor operation = operations.computeIfAbsent(type, t -> {
      for (Map.Entry<Class<? extends Operation>, OperationExecutor> entry : operations.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          return entry.getValue();
        }
      }
      return allOperation;
    });

    if (operation == null) {
      throw new IllegalArgumentException("unknown operation type: " + type);
    }
    return operation;
  }

  /**
   * Operation executor.
   */
  private class OperationExecutor implements Function<Commit<?>, CompletableFuture<Object>> {
    private final Method method;
    private final boolean async;

    private OperationExecutor(Method method) {
      this.method = method;
      async = method.getReturnType() == CompletableFuture.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Object> apply(Commit<?> commit) {
      if (Command.class.isAssignableFrom(commit.type())) {
        setTime(commit.timestamp());
      }

      try {
        return async ? (CompletableFuture<Object>) method.invoke(StateMachine.this, commit) : CompletableFuture.completedFuture(method.invoke(StateMachine.this, commit));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new ApplicationException("failed to invoke operation", e);
      }
    }
  }

  /**
   * Sets the state machine time.
   */
  private void setTime(long time) {
    this.time = time;
  }

  /**
   * Returns the current state machine time.
   *
   * @return The current state machine time.
   */
  protected long getTime() {
    return time;
  }

  /**
   * Updates the state machine time.
   *
   * @param timestamp The state machine timestamp.
   */
  public void tick(long timestamp) {
    setTime(timestamp);
  }

  /**
   * Called when a new session is registered.
   *
   * @param session The session that was registered.
   */
  public void register(Session session) {

  }

  /**
   * Applies an operation to the state machine.
   *
   * @param commit The commit to apply.
   * @return The operation result.
   */
  public CompletableFuture<Object> apply(Commit<? extends Operation> commit) {
    LOGGER.debug("Applying {}", commit);
    return findOperation(commit.type()).apply(commit);
  }

  /**
   * Called when a session is expired.
   *
   * @param session The expired session.
   */
  public void expire(Session session) {

  }

  /**
   * Called when a session is closed.
   *
   * @param session The session that was closed.
   */
  public void close(Session session) {

  }

  /**
   * Closes the state machine.
   */
  @Override
  public void close() {

  }

}
