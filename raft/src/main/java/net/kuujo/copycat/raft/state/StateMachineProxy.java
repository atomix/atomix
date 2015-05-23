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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Resource state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineProxy {
  private final StateMachine stateMachine;
  private final RaftContext state;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private final Map<Class<? extends Command>, Method> commands = new HashMap<>();
  private Method allCommand;

  public StateMachineProxy(StateMachine stateMachine, RaftContext state, ExecutionContext context) {
    this.stateMachine = stateMachine;
    this.state = state;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    for (Method method : stateMachine.getClass().getMethods()) {
      Apply apply = method.getAnnotation(Apply.class);
      if (apply != null) {
        for (Class<? extends Command> command : apply.value()) {
          if (command == Apply.All.class) {
            allCommand = method;
          } else {
            commands.put(command, method);
          }
        }
      }
    }
  }

  /**
   * Applies a command to the state machine.
   *
   * @param index The command index.
   * @param timestamp The command timestamp.
   * @param command The command to apply.
   * @return A completable future to be completed once the command has been applied.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Object> apply(long index, long timestamp, Command command) {
    return CompletableFuture.supplyAsync(() -> {
      if (index <= state.getLastApplied())
        throw new IllegalStateException("cannot apply old command");
      Object result = apply(new Commit(index, timestamp, command));
      state.setLastApplied(index);
      return result;
    }, context);
  }

  /**
   * Applies a query to the state machine.
   *
   * @param index The query index.
   * @param timestamp The query timestamp.
   * @param query The query to apply.
   * @return A completable future to be completed once the query has been applied.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Object> apply(long index, long timestamp, Query query) {
    return CompletableFuture.supplyAsync(() -> {
      // In order to satisfy linearizability requirements, we only need to ensure that the query is not applied at
      // a state prior to the given index.
      if (index < state.getLastApplied())
        throw new IllegalStateException("cannot apply old query");
      return apply(new Commit(index, timestamp, query));
    }, context);
  }

  /**
   * Applies a commit to the state machine.
   *
   * @param commit The commit to apply.
   * @return The commit result.
   */
  private Object apply(Commit commit) {
    Method method = commands.get(commit.type());
    if (method == null) {
      method = allCommand;
      if (method == null) {
        throw new IllegalArgumentException("unknown command type:" + commit.type());
      }
    }

    try {
      return method.invoke(stateMachine, commit);
    } catch (IllegalAccessException | InvocationTargetException e) {
      return new ApplicationException("failed to invoke command", e);
    }
  }

}
