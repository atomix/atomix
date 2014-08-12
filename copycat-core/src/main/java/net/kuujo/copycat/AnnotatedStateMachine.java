/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Command.Argument;

/**
 * State machine implementation that supports CopyCat command annotations.<p>
 *
 * Users should extend this class in order to implement annotation-based
 * state machines. The annotated state machine supports all CopyCat features
 * through annotations, such as command types and named arguments.<p>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AnnotatedStateMachine implements StateMachine, CommandProvider {
  private final Map<String, CommandHolder> commands = new HashMap<>();
  private final Map<String, Field> stateFields = new HashMap<>();

  private static class CommandHolder {
    private final AnnotatedStateMachine stateMachine;
    private final Command info;
    private final Method method;
    private CommandHolder(AnnotatedStateMachine stateMachine, Command info, Method method) {
      this.stateMachine = stateMachine;
      this.info = info;
      this.method = method;
    }
    private Object call(Map<String, Object> args) {
      try {
        return method.invoke(stateMachine, buildArguments(args));
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new CopyCatException(e);
      }
    }
    private Object[] buildArguments(Map<String, Object> arguments) {
      Parameter[] params = method.getParameters();
      Object[] args = new Object[params.length];
      for (int i = 0; i < params.length; i++) {
        Parameter param = params[i];
        Argument argument = param.getAnnotation(Argument.class);
        if (argument != null) {
          Object value = arguments.get(argument.value());
          if (value == null && argument.required()) {
            throw new IllegalArgumentException("Missing required argument " + argument.value());
          }
          args[i] = value;
        } else {
          args[i] = null;
        }
      }
      return args;
    }
  }

  public AnnotatedStateMachine() {
    init();
  }

  /**
   * Initializes all internal commands.
   */
  private void init() {
    Class<?> clazz = getClass();
    while (clazz != Object.class) {
      for (Method method : clazz.getDeclaredMethods()) {
        Command command = method.getAnnotation(Command.class);
        if (command != null) {
          String name = command.name();
          if (name.equals("")) {
            name = method.getName();
          }
          if (!commands.containsKey(name)) {
            commands.put(name, new CommandHolder(this, new GenericCommand(name, command.type()), method));
          }
        }
      }
      for (Field field : clazz.getDeclaredFields()) {
        Stateful stateful = field.getAnnotation(Stateful.class);
        if (stateful != null && !stateFields.containsKey(field.getName())) {
          stateFields.put(field.getName(), field);
        }
      }
      clazz = clazz.getSuperclass();
    }
  }

  @Override
  public Command getCommand(String name) {
    CommandHolder command = commands.get(name);
    return command != null ? command.info : null;
  }

  @Override
  public Map<String, Object> takeSnapshot() {
    Map<String, Object> snapshot = new HashMap<>();
    for (Map.Entry<String, Field> entry : stateFields.entrySet()) {
      entry.getValue().setAccessible(true);
      try {
        snapshot.put(entry.getKey(), entry.getValue().get(this));
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new CopyCatException(e);
      }
    }
    return snapshot;
  }

  @Override
  public void installSnapshot(Map<String, Object> snapshot) {
    for (String key : snapshot.keySet()) {
      Field field = stateFields.get(key);
      if (field != null) {
        field.setAccessible(true);
        try {
          field.set(this, snapshot.get(key));
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new CopyCatException(e);
        }
      }
    }
  }

  @Override
  public Object applyCommand(String name, Map<String, Object> args) {
    CommandHolder command = commands.get(name);
    if (command != null) {
      return command.call(args);
    }
    return null;
  }

}
