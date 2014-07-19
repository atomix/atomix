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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
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

import net.kuujo.copycat.util.serializer.Serializer;

/**
 * State machine implementation for Java-based annotated state machines.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AnnotatedStateMachine implements StateMachine, CommandProvider {
  private static final Serializer serializer = Serializer.getInstance();
  private Map<String, CommandWrapper> commands;
  private Collection<BeforeWrapper> before;
  private Collection<AfterWrapper> after;
  private SnapshotTaker snapshotTaker;
  private SnapshotInstaller snapshotInstaller;

  public AnnotatedStateMachine() {
    introspect(getClass());
  }

  /**
   * Intiializes the state machine.
   */
  private void introspect(Class<?> clazz) {
    final AnnotationIntrospector introspector = new AnnotationIntrospector();
    this.commands = new HashMap<>();
    for (CommandWrapper command : introspector.findCommands(clazz)) {
      this.commands.put(command.name, command);
    }
    this.before = introspector.findBefore(clazz);
    this.after = introspector.findAfter(clazz);
    this.snapshotTaker = introspector.findSnapshotTaker(clazz);
    this.snapshotInstaller = introspector.findSnapshotInstaller(clazz);
  }

  @Override
  public Map<String, Object> createSnapshot() {
    if (snapshotTaker != null) {
      try {
        return snapshotTaker.call(this, null);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void installSnapshot(Map<String, Object> snapshot) {
    if (snapshotInstaller != null) {
      try {
        snapshotInstaller.call(this, serializer.readValue(snapshot.toString().getBytes(), Map.class));
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public CommandInfo getCommandInfo(String name) {
    CommandWrapper command = commands.get(name);
    return command != null ? command.info : null;
  }

  @Override
  public Map<String, Object> applyCommand(String name, Map<String, Object> args) {
    for (BeforeWrapper before : this.before) {
      try {
        before.call(this, name);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    Map<String, Object> result = null;
    CommandWrapper command = commands.get(name);
    if (command != null) {
      try {
        result = command.call(this, args);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(e);
      }
    }

    for (AfterWrapper after : this.after) {
      try {
        after.call(this, name);
      }
      catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
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
          if (!before.containsKey(method.getName()) && method.isAnnotationPresent(CommandInfo.Before.class)) {
            before.put(method.getName(), new BeforeWrapper(method.getAnnotation(CommandInfo.Before.class), method));
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
          if (!after.containsKey(method.getName()) && method.isAnnotationPresent(CommandInfo.After.class)) {
            after.put(method.getName(), new AfterWrapper(method.getAnnotation(CommandInfo.After.class), method));
          }
        }
        current = current.getSuperclass();
      }
      return after.values();
    }

    /**
     * Creates a snapshot taker.
     */
    private SnapshotTaker findSnapshotTaker(Class<?> clazz) {
      return new SnapshotTaker(findProperties(clazz));
    }

    /**
     * Creates a snapshot installer.
     */
    private SnapshotInstaller findSnapshotInstaller(Class<?> clazz) {
      return new SnapshotInstaller(findProperties(clazz));
    }

    /**
     * Finds a collection of properties on the type.
     */
    private Collection<StatefulProperty> findProperties(Class<?> clazz) {
      final Map<String, StatefulProperty> properties = new HashMap<>();

      Class<?> current = clazz;
      while (current != Object.class) {
        for (Field field : current.getDeclaredFields()) {
          if (field.isAnnotationPresent(Stateful.class)) {

            // Get the property name. If the annotation value is empty then
            // the name should be derived from the field name.
            String name = field.getAnnotation(Stateful.class).value();
            if (name.equals("")) {
              name = field.getName();
            }

            // If this property has already been set then ignore this one.
            if (properties.containsKey(name)) {
              continue;
            }

            Method getter = null;
            Method setter = null;

            // Try to find getter and setter methods for the property.
            // First attempt to find setters and getters with explicitly
            // named Stateful annotations.
            for (Method method : current.getDeclaredMethods()) {
              if (method.isAnnotationPresent(Stateful.class) && method.getAnnotation(Stateful.class).value().equals(name)) {
                // If we found a Stateful annotation, try to determine
                // whether it's a getter or a setter based on arguments
                // and return values.
                if (method.getParameterTypes().length == 1) {
                  setter = method;
                }
                else if (method.getParameterTypes().length == 0 && !method.getReturnType().equals(Void.TYPE)) {
                  getter = method;
                }
              }
            }

            // Finally, if getters or setters are missing then try to find
            // them using a property descriptor.
            try {
              PropertyDescriptor property = findPropertyInfo(field);
              if (property != null) {
                if (getter == null) {
                  getter = property.getReadMethod();
                }
                if (setter == null) {
                  setter = property.getWriteMethod();
                }
              }
            }
            catch (IntrospectionException e) {
            }

            properties.put(name, new StatefulProperty(name, field, getter, setter));
          }
        }

        for (Method method : current.getDeclaredMethods()) {
          if (method.isAnnotationPresent(Stateful.class)) {
            String name = method.getAnnotation(Stateful.class).value();
            if (!name.equals("") && !properties.containsKey(name)) {
              Method getter = null;
              Method setter = null;

              if (method.getParameterTypes().length == 1) {
                setter = method;
                for (Method method2 : current.getDeclaredMethods()) {
                  if (!method2.equals(method) && method2.isAnnotationPresent(Stateful.class)
                      && method2.getAnnotation(Stateful.class).value().equals(name)
                      && method2.getParameterTypes().length == 0
                      && !method2.getReturnType().equals(Void.TYPE)) {
                    getter = method2;
                  }
                }
              }
              else if (method.getParameterTypes().length == 0 && !method.getReturnType().equals(Void.TYPE)) {
                getter = method;
                for (Method method2 : current.getDeclaredMethods()) {
                  if (!method2.equals(method) && method2.isAnnotationPresent(Stateful.class)
                      && method2.getAnnotation(Stateful.class).value().equals(name)
                      && method2.getParameterTypes().length == 1) {
                    setter = method2;
                  }
                }
              }

              if (getter != null && setter != null) {
                properties.put(name, new StatefulProperty(name, null, getter, setter));
              }
            }
          }
        }

        current = current.getSuperclass();
      }
      return properties.values();
    }

    /**
     * Finds property info for an annotated field.
     */
    private PropertyDescriptor findPropertyInfo(Field field) throws IntrospectionException {
      Class<?> clazz = field.getDeclaringClass();
      BeanInfo info = Introspector.getBeanInfo(clazz);
      for (PropertyDescriptor property : info.getPropertyDescriptors()) {
        if (field.getName().equals(property.getName())) {
          return property;
        }
      }
      return null;
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
          if (method.isAnnotationPresent(CommandInfo.class)) {
            CommandInfo info = method.getAnnotation(CommandInfo.class);
            String name = info.name();
            if (name.equals("")) {
              name = method.getName();
            }

            // If a command with this name was already added then skip it.
            if (commands.containsKey(name)) {
              continue;
            }

            if (method.isAnnotationPresent(CommandInfo.Arguments.class)) {
              commands.put(name, new CommandWrapper(name, info, method.getAnnotation(CommandInfo.Arguments.class).value(), method));
              continue;
            }
  
            Annotation[][] params = method.getParameterAnnotations();
            if (params.length == 0) {
              commands.put(name, new CommandWrapper(name, info, new CommandInfo.Argument[0], method));
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
                if (CommandInfo.Argument.class.isAssignableFrom(annotation.getClass())) {
                  hasAnnotation = true;
                  break;
                }
              }
              if (!hasAnnotation) {
                if (Map.class.isAssignableFrom(method.getParameterTypes()[0])) {
                  commands.put(name, new ObjectCommandWrapper(name, info, new CommandInfo.Argument[]{new DefaultArgument()}, method));
                  continue;
                }
              }
            }

            // If we've made it this far, then method parameters should be
            // explicitly annotated.
            final List<CommandInfo.Argument> arguments = new ArrayList<>();
            for (int i = 0; i < params.length; i++) {
              // Try to find an Argument annotation on the parameter.
              boolean hasAnnotation = false;
              for (Annotation annotation : params[i]) {
                if (CommandInfo.Argument.class.isAssignableFrom(annotation.getClass())) {
                  arguments.add((CommandInfo.Argument) annotation);
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

            commands.put(name, new CommandWrapper(name, info, arguments.toArray(new CommandInfo.Argument[arguments.size()]), method));
          }
        }
        current = current.getSuperclass();
      }
      return commands.values();
    }

    /**
     * A default command argument.
     */
    @SuppressWarnings("all")
    private static class DefaultArgument implements Annotation, CommandInfo.Argument {
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
        return CommandInfo.Argument.class;
      }
    }

    /**
     * A named command argument.
     */
    @SuppressWarnings("all")
    private static class NamedArgument implements Annotation, CommandInfo.Argument {
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
        return CommandInfo.Argument.class;
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
  private static class CommandWrapper implements Function<Map<String, Object>, Map<String, Object>> {
    private static final Serializer serializer = Serializer.getInstance();
    protected final String name;
    protected final CommandInfo info;
    protected final Annotation[] args;
    protected final Method method;
    protected final Class<?>[] parameters;

    private CommandWrapper(String name, CommandInfo info, Annotation[] args, Method method) {
      this.name = name;
      this.info = info;
      this.args = args;
      this.method = method;
      parameters = method.getParameterTypes();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> call(Object obj, Map<String, Object> arg) throws IllegalAccessException, InvocationTargetException {
      Object[] args = new Object[this.args.length];
      for (int i = 0; i < this.args.length; i++) {
        Annotation annotation = this.args[i];
        if (annotation instanceof CommandInfo.Argument) {
          CommandInfo.Argument argument = (CommandInfo.Argument) annotation;
          String name = argument.value();
  
          // If no argument name was provided then this indicates no actual
          // annotation present on the argument. We pass the entire JsonObject.
          if (name == null) {
            args[i] = arg;
          } else if (arg.containsKey(name)) {
            Object value = arg.get(name);
            if (value == null) {
              if (argument.required()) {
                throw new IllegalArgumentException("Invalid argument " + name);
              } else {
                args[i] = value;
              }
            } else {
              if (parameters[i].isPrimitive()) {
                args[i] = value;
              } else if (value instanceof Map) {
                if (Map.class.isAssignableFrom(parameters[i])) {
                  args[i] = value;
                } else {
                  args[i] = serializer.readValue(serializer.writeValue(value), parameters[i]);
                }
              } else if (value instanceof List) {
                if (List.class.isAssignableFrom(parameters[i])) {
                  args[i] = value;
                } else {
                  args[i] = serializer.readValue(serializer.writeValue(value), parameters[i]);
                }
              } else {
                args[i] = value;
              }
            }
          } else if (!argument.required()) {
            // If the argument's missing from the JsonObject but it's not required
            // then just pass a null value.
            // If the type is a number then don't assign a null value to it.
            if (parameters[i].isPrimitive()) {
              try {
                args[i] = parameters[i].newInstance();
              } catch (InstantiationException e) {
                args[i] = 0;
              }
            } else {
              args[i] = null;
            }
          } else {
            // If the argument's missing from the JsonObject but is required then
            // throw an IllegalArgumentException.
            throw new IllegalArgumentException("Missing required argument " + name);
          }
        } else if (annotation instanceof CommandInfo.Value) {
          args[i] = arg;
        }
      }
      return (Map<String, Object>) method.invoke(obj, args);
    }
  }

  /**
   * Object command wrapper.
   */
  private static class ObjectCommandWrapper extends CommandWrapper {
    private ObjectCommandWrapper(String name, CommandInfo info, CommandInfo.Argument[] args, Method method) {
      super(name, info, args, method);
    }
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> call(Object object, Map<String, Object> arg) throws IllegalAccessException, InvocationTargetException {
      return (Map<String, Object>) method.invoke(object, arg);
    }
  }

  /**
   * Before wrapper.
   */
  private static class BeforeWrapper implements Function<String, Void> {
    private final Method method;
    private final Set<String> commands;

    private BeforeWrapper(CommandInfo.Before info, Method method) {
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

    private AfterWrapper(CommandInfo.After info, Method method) {
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
  private static class SnapshotTaker implements Function<Void, Map<String, Object>> {
    private final Collection<StatefulProperty> properties;

    private SnapshotTaker(Collection<StatefulProperty> properties) {
      this.properties = properties;
    }

    @Override
    public Map<String, Object> call(Object obj, Void arg) throws IllegalAccessException, InvocationTargetException {
      Map<String, Object> state = new HashMap<>();
      for (StatefulProperty property : properties) {
        state.put(property.name, property.get(obj));
      }
      return state;
    }
  }

  /**
   * Base class for snapshot installer functions.
   */
  private static class SnapshotInstaller implements Function<Map<String, byte[]>, Void> {
    private final Collection<StatefulProperty> properties;

    private SnapshotInstaller(Collection<StatefulProperty> properties) {
      this.properties = properties;
    }

    @Override
    public Void call(Object obj, Map<String, byte[]> data) throws IllegalAccessException, InvocationTargetException {
      for (StatefulProperty property : properties) {
        if (data.containsKey(property.name)) {
          byte[] value = data.get(property.name);
          if (value != null) {
            property.set(obj, value);
          }
        }
      }
      return null;
    }
  }

  /**
   * A stateful property.
   */
  private static class StatefulProperty {
    private final Serializer serializer = Serializer.getInstance();
    private final String name;
    private final Field field;
    private final Method getter;
    private final Method setter;

    private StatefulProperty(String name, Field field, Method getter, Method setter) {
      this.name = name;
      this.field = field;
      this.getter = getter;
      this.setter = setter;

      if (field != null) {
        field.setAccessible(true);
      }
    }

    private byte[] get(Object obj) throws IllegalAccessException, InvocationTargetException {
      if (getter != null) {
        return serializer.writeValue(getter.invoke(obj));
      }
      else {
        return serializer.writeValue(field.get(obj));
      }
    }

    private void set(Object obj, byte[] value) throws IllegalAccessException, InvocationTargetException {
      if (setter != null && setter.getParameterTypes().length > 0) {
        setter.invoke(obj, serializer.readValue(value, setter.getParameterTypes()[0]));
      }
      else {
        field.set(obj, serializer.readValue(value, field.getType()));
      }
    }

  }

}
