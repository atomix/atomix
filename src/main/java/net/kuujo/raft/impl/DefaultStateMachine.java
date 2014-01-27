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
package net.kuujo.raft.impl;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.json.JsonObject;

import net.kuujo.raft.Command;
import net.kuujo.raft.Function;
import net.kuujo.raft.StateMachine;

/**
 * A default state machine implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultStateMachine implements StateMachine {
  private Map<String, CommandInfo> commands = new HashMap<>();

  @Override
  public StateMachine registerCommand(String command, Command.Type type, Function<Command, JsonObject> function) {
    commands.put(command, new CommandInfo(type, function));
    return this;
  }

  @Override
  public StateMachine unregisterCommand(String command) {
    commands.remove(command);
    return this;
  }

  @Override
  public boolean hasCommand(String command) {
    return commands.containsKey(command);
  }

  @Override
  public Command.Type getCommandType(String command) {
    return commands.containsKey(command) ? commands.get(command).type : null;
  }

  @Override
  public JsonObject applyCommand(Command command) {
    if (commands.containsKey(command.command())) {
      return commands.get(command.command()).function.call(command);
    }
    return null;
  }

  private static class CommandInfo {
    private final Command.Type type;
    private final Function<Command, JsonObject> function;
    private CommandInfo(Command.Type type, Function<Command, JsonObject> function) {
      this.type = type;
      this.function = function;
    }
  }

}
