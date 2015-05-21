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
package net.kuujo.copycat.resource;

import com.google.common.base.CaseFormat;
import net.kuujo.copycat.ConfigurationException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resource proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceProxy implements InvocationHandler {
  private static Map<String, Map<Method, CommandInfo>> COMMANDS = new ConcurrentHashMap<>();
  private final CommitLog log;
  private final Map<Method, CommandInfo> commands;

  public ResourceProxy(Class<? extends Resource> type, CommitLog log) {
    this.log = log;

    Map<Method, CommandInfo> commands = COMMANDS.get(type.getName());

    if (commands == null) {

      commands = new ConcurrentHashMap<>();
      for (Method method : type.getMethods()) {

        Submit submit = method.getAnnotation(Submit.class);
        if (submit != null) {

          CommandInfo command = new CommandInfo(submit.value(), new Method[method.getParameterCount()]);
          Parameter[] params = method.getParameters();
          for (int i = 0; i < params.length; i++) {

            Parameter param = params[i];
            Submit.Argument arg = param.getAnnotation(Submit.Argument.class);
            if (arg != null) {
              try {
                command.args[i] = submit.value().getMethod("set" + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, arg.value()), param.getType());
              } catch (NoSuchMethodException e) {
                throw new ConfigurationException("missing setter for command argument: " + arg.value(), e);
              }
            }
          }

          commands.put(method, command);
        }
      }

      COMMANDS.put(type.getName(), commands);
    }

    this.commands = commands;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    CommandInfo command = commands.get(method);
    if (command == null)
      throw new ResourceException("unknown proxy command: " + method);
    return log.submit(command.command, (CommandProcessor) instance -> applyArgs(instance, command, args));
  }

  /**
   * Applies arguments to the given command.
   */
  private Command applyArgs(Command command, CommandInfo info, Object[] args) {
    for (int i = 0; i < info.args.length; i++) {
      Method method = info.args[i];
      Object arg = args[i];
      try {
        method.invoke(command, arg);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new ResourceException("failed to apply command arguments", e);
      }
    }
    return command;
  }

  /**
   * Command info.
   */
  private static class CommandInfo {
    private final Class<? extends Command> command;
    private Method[] args;

    private CommandInfo(Class<? extends Command> command, Method[] args) {
      this.command = command;
      this.args = args;
    }
  }

}
