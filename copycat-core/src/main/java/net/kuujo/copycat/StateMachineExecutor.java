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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * State machine executor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class StateMachineExecutor {
  private final StateMachine stateMachine;
  private final Map<String, CommandHolder> commands = new HashMap<>();
  private final Map<String, Field> stateFields = new HashMap<>();

  private static class CommandHolder {
    private final StateMachine stateMachine;
    private final Command info;
    private final List<Method> methods;
    private CommandHolder(StateMachine stateMachine, Command info, List<Method> methods) {
      this.stateMachine = stateMachine;
      this.info = info;
      this.methods = methods;
    }
    private Object call(List<Object> args) {
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
              return method.invoke(stateMachine, args.toArray());
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
              throw new CopyCatException(e);
            }
          }
        }

      throw new CopyCatException("Invalid command");
    }
  }

  StateMachineExecutor(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
    init();
  }

  /**
   * Initializes all internal commands.
   */
  private void init() {
    Class<?> clazz = stateMachine.getClass();
    while (clazz != Object.class) {
      for (Method method : clazz.getDeclaredMethods()) {
        Command command = method.getAnnotation(Command.class);
        if (command != null) {
          String name = command.name();
          if (name.equals("")) {
            name = method.getName();
          }
          CommandHolder holder = commands.get(name);
          if (holder == null) {
            holder = new CommandHolder(stateMachine, new GenericCommand(name, command.type()), new ArrayList<Method>());
            commands.put(name, holder);
          }
          holder.methods.add(method);
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

  /**
   * Returns command info for a named command.
   *
   * @param name The name of the command for which to return info.
   * @return The command info.
   */
  Command getCommand(String name) {
    CommandHolder command = commands.get(name);
    return command != null ? command.info : null;
  }

  /**
   * Returns a snapshot of the state machine state.
   *
   * @return The state machine snapshot.
   */
  Map<String, Object> takeSnapshot() {
    Map<String, Object> snapshot = new HashMap<>();
    for (Map.Entry<String, Field> entry : stateFields.entrySet()) {
      entry.getValue().setAccessible(true);
      try {
        snapshot.put(entry.getKey(), entry.getValue().get(stateMachine));
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new CopyCatException(e);
      }
    }
    return snapshot;
  }

  /**
   * Installs a snapshot of the state machine state.
   *
   * @param snapshot The snapshot to install.
   */
  void installSnapshot(Map<String, Object> snapshot) {
    for (String key : snapshot.keySet()) {
      Field field = stateFields.get(key);
      if (field != null) {
        field.setAccessible(true);
        try {
          field.set(stateMachine, snapshot.get(key));
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new CopyCatException(e);
        }
      }
    }
  }

  /**
   * Exceutes a state machine command.
   *
   * @param name The name of the command to execute.
   * @param args The command arguments.
   * @return The command return value.
   */
  Object applyCommand(String name, List<Object> args) {
    CommandHolder command = commands.get(name);
    if (command != null) {
      return command.call(args);
    }
    return null;
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
    private final Command.Type type;
  
    public GenericCommand(String name, Command.Type type) {
      this.name = name;
      this.type = type;
    }
  
    @Override
    public String name() {
      return name;
    }
  
    @Override
    public Command.Type type() {
      return type;
    }
  
    @Override
    public Class<? extends Annotation> annotationType() {
      return Command.class;
    }
  }

}
