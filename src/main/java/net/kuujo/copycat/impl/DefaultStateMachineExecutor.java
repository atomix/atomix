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
package net.kuujo.copycat.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.annotations.StateField;
import net.kuujo.copycat.annotations.StateFieldGetter;
import net.kuujo.copycat.annotations.StateFieldSetter;
import net.kuujo.copycat.annotations.StateGetter;
import net.kuujo.copycat.annotations.StateSetter;
import net.kuujo.copycat.annotations.StateValue;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.state.StateMachineExecutor;

/**
 * An internal state machine adapter.
 *
 * @author Jordan Halterman
 */
public class DefaultStateMachineExecutor implements StateMachineExecutor {
  private final StateMachine stateMachine;
  private Map<String, CommandWrapper> commands;
  private Collection<BeforeWrapper> before;
  private Collection<AfterWrapper> after;
  private SnapshotTaker snapshotTaker;
  private SnapshotInstaller snapshotInstaller;

  public DefaultStateMachineExecutor(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
    introspect(stateMachine.getClass());
  }

  /**
   * Intiializes the state machine.
   */
  private void introspect(Class<?> clazz) {
    final AnnotationIntrospector introspector = new AnnotationIntrospector();
    this.commands = new HashMap<>();
    for (CommandWrapper command : introspector.findCommands(clazz)) {
      this.commands.put(command.info.name(), command);
    }
    this.before = introspector.findBefore(clazz);
    this.after = introspector.findAfter(clazz);
    this.snapshotTaker = introspector.findSnapshotTaker(clazz);
    this.snapshotInstaller = introspector.findSnapshotInstaller(clazz);
  }

  @Override
  public Collection<Command> getCommands() {
    List<Command> commands = new ArrayList<>();
    for (CommandWrapper command : this.commands.values()) {
      commands.add(command.info);
    }
    return commands;
  }

  @Override
  public Command getCommand(String name) {
    return commands.get(name).info;
  }

  @Override
  public boolean hasCommand(String name) {
    return commands.containsKey(name);
  }

  @Override
  public Object applyCommand(String name, JsonObject args) {
    for (BeforeWrapper before : this.before) {
      try {
        before.call(stateMachine, name);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    Object result = null;
    CommandWrapper command = commands.get(name);
    if (command != null) {
      try {
        result = command.call(stateMachine, args.toMap());
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(e);
      }
    }

    for (AfterWrapper after : this.after) {
      try {
        after.call(stateMachine, name);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  @Override
  public JsonElement takeSnapshot() {
    if (snapshotTaker != null) {
      try {
        return snapshotTaker.call(stateMachine, null);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public void installSnapshot(JsonElement snapshot) {
    if (snapshotInstaller != null) {
      try {
        snapshotInstaller.call(stateMachine, snapshot);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        // Do nothing.
      }
    }
  }

  /**
   * A state machine annotation introspector.
   *
   * @author Jordan Halterman
   */
  private static class AnnotationIntrospector {
  
    /**
     * Finds methods to run before commands.
     */
    private Collection<BeforeWrapper> findBefore(Class<?> clazz) {
      final Map<String, BeforeWrapper> before = new HashMap<>();
      Class<?> current = clazz;
      while (current != Object.class) {
        for (Method method : current.getDeclaredMethods()) {
          if (!before.containsKey(method.getName()) && method.isAnnotationPresent(Command.Before.class)) {
            before.put(method.getName(), new BeforeWrapper(method.getAnnotation(Command.Before.class), method));
          }
        }
        current = current.getSuperclass();
      }
      return before.values();
    }

    /**
     * Finds methods to run after commands.
     */
    private Collection<AfterWrapper> findAfter(Class<?> clazz) {
      final Map<String, AfterWrapper> after = new HashMap<>();
      Class<?> current = clazz;
      while (current != Object.class) {
        for (Method method : current.getDeclaredMethods()) {
          if (!after.containsKey(method.getName()) && method.isAnnotationPresent(Command.After.class)) {
            after.put(method.getName(), new AfterWrapper(method.getAnnotation(Command.After.class), method));
          }
        }
        current = current.getSuperclass();
      }
      return after.values();
    }

    /**
     * Finds a snapshot taker.
     */
    private SnapshotTaker findSnapshotTaker(Class<?> clazz) {
      Class<?> current = clazz;
      Map<String, NamedStateSnapshotTaker> takers = null;
      while (current != Object.class) {
        if (takers == null) {
          // First search for a state getter method.
          for (Method method : current.getDeclaredMethods()) {
            if (method.isAnnotationPresent(StateGetter.class) && method.getParameterTypes().length == 0
                && !method.getReturnType().equals(Void.TYPE)) {
              return new StateGetterSnapshotTaker(method);
            }
          }

          // Next search for a state field.
          for (Field field : current.getDeclaredFields()) {
            if (field.isAnnotationPresent(StateValue.class)) {
              return new StateFieldSnapshotTaker(field);
            }
          }
        }

        // Now search for getter methods and field-level fields.
        for (Method method : current.getDeclaredMethods()) {
          if (method.isAnnotationPresent(StateFieldGetter.class) && method.getParameterTypes().length == 0
              && !method.getReturnType().equals(Void.TYPE)) {
            StateFieldGetter getter = method.getAnnotation(StateFieldGetter.class);
            if (takers == null) {
              takers = new HashMap<>();
            }
            if (!takers.containsKey(getter.value())) {
              takers.put(getter.value(), new NamedGetterSnapshotTaker(getter.value(), method));
            }
          }
        }

        // Search for field-level fields.
        for (Field field : current.getDeclaredFields()) {
          if (field.isAnnotationPresent(StateField.class)) {
            StateField stateField = field.getAnnotation(StateField.class);
            if (takers == null) {
              takers = new HashMap<>();
            }
            if (!takers.containsKey(stateField.value())) {
              takers.put(stateField.value(), new NamedFieldSnapshotTaker(stateField.value(), field));
            }
          }
        }

        current = current.getSuperclass();
      }
      return takers != null ? new StateFieldsSnapshotTaker(takers.values()) : null;
    }

    /**
     * Finds a snapshot installer.
     */
    private SnapshotInstaller findSnapshotInstaller(Class<?> clazz) {
      Class<?> current = clazz;
      Map<String, NamedStateSnapshotInstaller> installers = null;
      while (current != Object.class) {
        if (installers == null) {
          // First search for a state setter method.
          for (Method method : current.getDeclaredMethods()) {
            if (method.isAnnotationPresent(StateSetter.class) && method.getParameterTypes().length == 1) {
              return new StateSetterSnapshotInstaller(method);
            }
          }

          // Next search for a state field.
          for (Field field : current.getDeclaredFields()) {
            if (field.isAnnotationPresent(StateValue.class)) {
              return new StateFieldSnapshotInstaller(field);
            }
          }
        }

        // Now search for setter methods and field-level fields.
        for (Method method : current.getDeclaredMethods()) {
          if (method.isAnnotationPresent(StateFieldSetter.class) && method.getParameterTypes().length == 1) {
            StateFieldSetter setter = method.getAnnotation(StateFieldSetter.class);
            if (installers == null) {
              installers = new HashMap<>();
            }
            if (!installers.containsKey(setter.value())) {
              installers.put(setter.value(), new NamedSetterSnapshotInstaller(setter.value(), method));
            }
          }
        }

        // Search for field-level fields.
        for (Field field : current.getDeclaredFields()) {
          if (field.isAnnotationPresent(StateField.class)) {
            StateField stateField = field.getAnnotation(StateField.class);
            if (installers == null) {
              installers = new HashMap<>();
            }
            if (!installers.containsKey(stateField.value())) {
              installers.put(stateField.value(), new NamedFieldSnapshotInstaller(stateField.value(), field));
            }
          }
        }

        current = current.getSuperclass();
      }
      return installers != null ? new StateFieldsSnapshotInstaller(installers.values()) : null;
    }

    /**
     * Finds all commands for the given class.
     *
     * @param clazz The class on which to find commands.
     * @return A collection of commands.
     */
    private Collection<CommandWrapper> findCommands(Class<?> clazz) {
      final Map<String, CommandWrapper> commands = new HashMap<>();
      Class<?> current = clazz;
      while (current != Object.class) {
        for (Method method : current.getDeclaredMethods()) {
          if (method.isAnnotationPresent(Command.class)) {
            Command info = method.getAnnotation(Command.class);
  
            // If a command with this name was already added then skip it.
            if (commands.containsKey(info.name())) {
              continue;
            }

            if (method.isAnnotationPresent(Command.Arguments.class)) {
              commands.put(info.name(), new CommandWrapper(info, method.getAnnotation(Command.Arguments.class).value(), method));
              continue;
            }
  
            Annotation[][] params = method.getParameterAnnotations();
            if (params.length == 0) {
              commands.put(info.name(), new CommandWrapper(info, new Command.Argument[0], method));
              continue;
            }

            // If the method has arguments, check if it only has one JsonObject
            // argument. If the single JsonObject argument desn't have an
            // annotation then create a placeholder annotation which will
            // indicate that the entire command arguments object should be
            // passed to the method.
            if (params.length == 1) {
              boolean hasAnnotation = false;
              for (Annotation annotation : params[0]) {
                if (Command.Argument.class.isAssignableFrom(annotation.getClass())) {
                  hasAnnotation = true;
                  break;
                }
              }
              if (!hasAnnotation) {
                if (JsonObject.class.isAssignableFrom(method.getParameterTypes()[0])) {
                  commands.put(info.name(), new ObjectCommandWrapper(info, new Command.Argument[]{new DefaultArgument()}, method));
                  continue;
                }
              }
            }

            // If we've made it this far, then method parameters should be
            // explicitly annotated.
            final List<Command.Argument> arguments = new ArrayList<>();
            for (int i = 0; i < params.length; i++) {
              // Try to find an Argument annotation on the parameter.
              boolean hasAnnotation = false;
              for (Annotation annotation : params[i]) {
                if (Command.Argument.class.isAssignableFrom(annotation.getClass())) {
                  arguments.add((Command.Argument) annotation);
                  hasAnnotation = true;
                  break;
                }
              }

              // If the parameter didn't have an Argument annotation then
              // create an Argument annotation based on the argument position.
              if (!hasAnnotation) {
                arguments.add(new NamedArgument(String.format("arg%d", i)));
              }
            }

            commands.put(info.name(), new CommandWrapper(info, arguments.toArray(new Command.Argument[arguments.size()]), method));
          }
        }
        current = current.getSuperclass();
      }
      return commands.values();
    }

    /**
     * A default command argument.
     */
    private static class DefaultArgument implements Annotation, Command.Argument {
      @Override
      public String value() {
        return null;
      }
      @Override
      public boolean required() {
        return true;
      }
      @Override
      public Class<? extends Annotation> annotationType() {
        return Command.Argument.class;
      }
    }

    /**
     * A named command argument.
     */
    private static class NamedArgument implements Annotation, Command.Argument {
      private final String name;
      private NamedArgument(String name) {
        this.name = name;
      }
      @Override
      public String value() {
        return name;
      }
      @Override
      public boolean required() {
        return true;
      }
      @Override
      public Class<? extends Annotation> annotationType() {
        return Command.Argument.class;
      }
    }

  }

  /**
   * A callable function.
   */
  private static interface Function<I, O> {

    /**
     * Calls the function.
     */
    public O call(Object obj, I arg) throws IllegalAccessException, InvocationTargetException;

  }

  /**
   * Command wrapper.
   */
  private static class CommandWrapper implements Function<Map<String, Object>, Object> {
    protected final Command info;
    protected final Annotation[] args;
    protected final Method method;

    private CommandWrapper(Command info, Annotation[] args, Method method) {
      this.info = info;
      this.args = args;
      this.method = method;
    }

    @Override
    public Object call(Object obj, Map<String, Object> arg) throws IllegalAccessException, InvocationTargetException {
      Object[] args = new Object[this.args.length];
      for (int i = 0; i < this.args.length; i++) {
        Annotation annotation = this.args[i];
        if (annotation instanceof Command.Argument) {
          Command.Argument argument = (Command.Argument) annotation;
          String name = argument.value();
  
          // If no argument name was provided then this indicates no actual
          // annotation present on the argument. We pass the entire JsonObject.
          if (name == null) {
            args[i] = arg;
          }
          // If the field exists in the JsonObject then extract it.
          else if (arg.containsKey(name)) {
            try {
              args[i] = arg.get(name);
            }
            catch (RuntimeException e) {
              // If the argument value is invalid then we may pass a null value in
              // instead if the argument isn't required.
              if (argument.required()) {
                throw new IllegalArgumentException("Invalid argument " + name);
              }
              else {
                args[i] = null;
              }
            }
          }
          // If the argument's missing from the JsonObject but it's not required
          // then just pass a null value.
          else if (!argument.required()) {
            args[i] = null;
          }
          // If the argument's missing from the JsonObject but is required then
          // throw an IllegalArgumentException.
          else {
            throw new IllegalArgumentException("Missing required argument " + name);
          }
        }
        else if (annotation instanceof Command.Value) {
          args[i] = arg;
        }
      }
      return method.invoke(obj, args);
    }
  }

  /**
   * Object command wrapper.
   */
  private static class ObjectCommandWrapper extends CommandWrapper {
    private ObjectCommandWrapper(Command info, Command.Argument[] args, Method method) {
      super(info, args, method);
    }
    @Override
    public Object call(Object object, Map<String, Object> arg) throws IllegalAccessException, InvocationTargetException {
      return method.invoke(object, new JsonObject(arg));
    }
  }

  /**
   * Before wrapper.
   */
  private static class BeforeWrapper implements Function<String, Void> {
    private final Method method;
    private final Set<String> commands;

    private BeforeWrapper(Command.Before info, Method method) {
      this.method = method;
      commands = new HashSet<>(Arrays.asList(info.value()));
    }

    @Override
    public Void call(Object obj, String command) throws IllegalAccessException, InvocationTargetException {
      if (commands.isEmpty() || commands.contains(command)) {
        method.invoke(obj);
      }
      return (Void) null;
    }
  }

  /**
   * After wrapper.
   */
  private static class AfterWrapper implements Function<String, Void> {
    private final Method method;
    private final Set<String> commands;

    private AfterWrapper(Command.After info, Method method) {
      this.method = method;
      commands = new HashSet<>(Arrays.asList(info.value()));
    }

    @Override
    public Void call(Object obj, String command) throws IllegalAccessException, InvocationTargetException {
      if (commands.isEmpty() || commands.contains(command)) {
        method.invoke(obj);
      }
      return (Void) null;
    }
  }

  /**
   * Base class for snapshot functions.
   */
  private static abstract class SnapshotTaker implements Function<Void, JsonElement> {
  }

  /**
   * A snapshot function that gets the snapshot from a full state getter.
   */
  private static class StateGetterSnapshotTaker extends SnapshotTaker {
    Serializer serializer = Serializer.getInstance();
    private final Method method;
    private StateGetterSnapshotTaker(Method method) {
      this.method = method;
      method.setAccessible(true);
    }
    @Override
    public JsonElement call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      return serializer.writeObject(method.invoke(obj));
    }
  }

  /**
   * A snapshot function that gets the snapshot from a full state field.
   */
  private static class StateFieldSnapshotTaker extends SnapshotTaker {
    Serializer serializer = Serializer.getInstance();
    private final Field field;
    private StateFieldSnapshotTaker(Field field) {
      this.field = field;
      field.setAccessible(true);
    }
    @Override
    public JsonElement call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      return serializer.writeObject(field.get(obj));
    }
  }

  /**
   * A snapshot function that combines the snapshot from a set of fields.
   */
  private static class StateFieldsSnapshotTaker extends SnapshotTaker {
    Serializer serializer = Serializer.getInstance();
    private Collection<NamedStateSnapshotTaker> snapshots;
    private StateFieldsSnapshotTaker(Collection<NamedStateSnapshotTaker> snapshots) {
      this.snapshots = snapshots;
    }
    @Override
    public JsonElement call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      Map<String, Object> data = new HashMap<>();
      for (NamedStateSnapshotTaker snapshot : snapshots) {
        data.put(snapshot.name, snapshot.call(obj, arg));
      }
      return serializer.writeObject(data);
    }
  }

  /**
   * Base class for field-based snapshot takers.
   */
  private static abstract class NamedStateSnapshotTaker implements Function<Void, Object> {
    private final String name;
    protected NamedStateSnapshotTaker(String name) {
      this.name = name;
    }
  }

  /**
   * A snapshot function that gets the snapshot field from a field value.
   */
  private static class NamedFieldSnapshotTaker extends NamedStateSnapshotTaker {
    private final Field field;
    protected NamedFieldSnapshotTaker(String name, Field field) {
      super(name);
      this.field = field;
    }
    @Override
    public Object call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      return field.get(obj);
    }
  }

  /**
   * A snapshot function that gets the snapshot field from a getter.
   */
  private static class NamedGetterSnapshotTaker extends NamedStateSnapshotTaker {
    private final Method method;
    protected NamedGetterSnapshotTaker(String name, Method method) {
      super(name);
      this.method = method;
    }
    @Override
    public Object call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      return method.invoke(obj);
    }
  }

  /**
   * Base class for snapshot installer functions.
   */
  private static abstract class SnapshotInstaller implements Function<JsonElement, Void> {
  }

  /**
   * A snapshot function that sets the snapshot from a full state setter.
   */
  private static class StateSetterSnapshotInstaller extends SnapshotInstaller {
    Serializer serializer = Serializer.getInstance();
    private final Method method;
    private final Class<?> type;
    private StateSetterSnapshotInstaller(Method method) {
      this.method = method;
      type = method.getParameterTypes()[0];
      method.setAccessible(true);
    }
    @Override
    public Void call(Object obj, JsonElement arg) throws IllegalAccessException, InvocationTargetException {
      method.invoke(obj, serializer.readObject(arg, type));
      return null;
    }
  }

  /**
   * A snapshot function that Sets the snapshot on a full state field.
   */
  private static class StateFieldSnapshotInstaller extends SnapshotInstaller {
    Serializer serializer = Serializer.getInstance();
    private final Field field;
    private StateFieldSnapshotInstaller(Field field) {
      this.field = field;
      field.setAccessible(true);
    }
    @Override
    public Void call(Object obj, JsonElement arg) throws IllegalAccessException, InvocationTargetException {
      field.set(obj, serializer.readObject(arg, field.getType()));
      return null;
    }
  }

  /**
   * A snapshot function that sets the snapshot on a group of fields and methods.
   */
  private static class StateFieldsSnapshotInstaller extends SnapshotInstaller {
    private Collection<NamedStateSnapshotInstaller> snapshots;
    private StateFieldsSnapshotInstaller(Collection<NamedStateSnapshotInstaller> snapshots) {
      this.snapshots = snapshots;
    }
    @Override
    public Void call(Object obj, JsonElement arg) throws IllegalAccessException, InvocationTargetException {
      JsonObject json = arg.asObject();
      for (NamedStateSnapshotInstaller snapshot : snapshots) {
        snapshot.call(obj, json.getValue(snapshot.name));
      }
      return null;
    }
  }

  /**
   * Base class for named field setters and getters.
   */
  private static abstract class NamedStateSnapshotInstaller implements Function<Object, Void> {
    private final String name;
    protected NamedStateSnapshotInstaller(String name) {
      this.name = name;
    }
  }

  /**
   * A snapshot function that sets a snapshot field on a field.
   */
  private static class NamedFieldSnapshotInstaller extends NamedStateSnapshotInstaller {
    private final Field field;
    protected NamedFieldSnapshotInstaller(String name, Field field) {
      super(name);
      this.field = field;
      field.setAccessible(true);
    }
    @Override
    public Void call(Object obj, Object arg) throws IllegalAccessException, InvocationTargetException {
      field.set(obj, arg);
      return null;
    }
    
  }

  /**
   * A snapshot function that sets a snapshot field with a setter.
   */
  private static class NamedSetterSnapshotInstaller extends NamedStateSnapshotInstaller {
    private final Method method;
    protected NamedSetterSnapshotInstaller(String name, Method method) {
      super(name);
      this.method = method;
      method.setAccessible(true);
    }
    @Override
    public Void call(Object obj, Object arg) throws IllegalAccessException, InvocationTargetException {
      method.invoke(obj, arg);
      return null;
    }
  }

}
