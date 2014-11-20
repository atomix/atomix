/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.Reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine executor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class StateMachineExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineExecutor.class);
  private final StateMachine stateMachine;
  private final Map<String, Operation> operations = new HashMap<>();

  /**
   * @throws NullPointerException if {@code stateMachine} is null
   */
  public StateMachineExecutor(StateMachine stateMachine) {
    this.stateMachine = Assert.isNotNull(stateMachine, "stateMachine");
    init();
  }

  /**
   * Initializes the executor, analyzing the state machine.
   */
  private void init() {
    Class<?> clazz = stateMachine.getClass();
    while (clazz != Object.class) {
      for (Method method : clazz.getDeclaredMethods()) {
        // Check whether this method is a "command" method.
        Command command = Reflection.findAnnotation(method, Command.class);
        if (command != null) {
          addCommandMethod(method, command);
        } else {
          // Check whether this method is a "query" method.
          Query query = Reflection.findAnnotation(method, Query.class);
          if (query != null) {
            addQueryMethod(method, query);
          }
        }
      }
      clazz = clazz.getSuperclass();
    }
  }

  /**
   * Adds a command method.
   */
  private void addCommandMethod(Method method, Command command) {
    String name = command.name();
    if (name.equals("")) {
      name = method.getName();
    }
    Operation operation = operations.get(name);
    if (operation == null) {
      operation = new Operation(stateMachine, false, new ArrayList<>(10));
      operations.put(name, operation);
    } else if (operation.isReadOnly()) {
      throw new IllegalStateException("Overloaded methods must be of the same type");
    }
    operation.methods.add(method);
  }

  /**
   * Adds a query method.
   */
  private void addQueryMethod(Method method, Query query) {
    String name = query.name();
    if (name.equals("")) {
      name = method.getName();
    }
    Operation operation = operations.get(name);
    if (operation == null) {
      operation = new Operation(stateMachine, true, new ArrayList<>(10));
      operations.put(name, operation);
    } else if (!operation.isReadOnly()) {
      throw new IllegalStateException("Overloaded methods must be of the same type");
    }
    operation.methods.add(method);
  }

  /**
   * Returns the underlying state machine.
   *
   * @return The underlying state machine.
   */
  public StateMachine stateMachine() {
    return stateMachine;
  }

  /**
   * Returns a named operation.
   *
   * @param name The operation name.
   * @return The state machine operation.
   */
  public Operation getOperation(String name) {
    return operations.get(name);
  }

  /**
   * State machine operation.
   */
  public static class Operation {
    private final StateMachine stateMachine;
    private final boolean readOnly;
    private final List<Method> methods;

    private Operation(StateMachine stateMachine, boolean readOnly, List<Method> methods) {
      this.stateMachine = stateMachine;
      this.readOnly = readOnly;
      this.methods = methods;
    }

    /**
     * Returns a boolean value indicating whether the operation is a read-only operation.
     */
    public boolean isReadOnly() {
      return readOnly;
    }

    /**
     * Applies the given arguments to the operation, returning the operation result.
     */
    public Object apply(List<Object> args) {
      int size = args.size();
      methodLoop:
        for (Method method : methods) {
          Class<?>[] paramTypes = method.getParameterTypes();
          if (paramTypes.length == size) {
            for (int i = 0; i < paramTypes.length; i++) {
              if (!paramTypes[i].isAssignableFrom(args.get(i).getClass())) {
                continue methodLoop;
              }
            }

            try {
              LOGGER.debug("{} calling {} with arguments: {}", stateMachine, method.getName(), args);
              return method.invoke(stateMachine, args.toArray());
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
              throw new CopycatException(e);
            }
          }
        }
      throw new CopycatException("Invalid command");
    }
  }

  /**
   * Generic command descriptor implementation.<p>
   *
   * This class can be used by {@link StateMachine} implementations to provide
   * command info for state machine commands.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  @SuppressWarnings("all")
  public static class GenericCommand implements Annotation, Command {
    private final String name;

    public GenericCommand(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
      return Command.class;
    }
  }

}
