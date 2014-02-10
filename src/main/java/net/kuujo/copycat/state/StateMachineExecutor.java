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
package net.kuujo.copycat.state;

import java.util.Collection;

import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.annotations.Command;

/**
 * A state machine executor.
 *
 * @author Jordan Halterman
 */
public interface StateMachineExecutor {

  /**
   * Returns a collection of all state machine commands.
   *
   * @return A collection of state machine commands.
   */
  Collection<Command> getCommands();

  /**
   * Returns a state machine command.
   *
   * @param name The command name.
   * @return A state machine command, or <code>null</code> if the command does
   *         not exist.
   */
  Command getCommand(String name);

  /**
   * Returns a boolean indicating whether a command exists.
   *
   * @param name The command name.
   * @return Indicates whether the given command exists.
   */
  boolean hasCommand(String name);

  /**
   * Applies a command to the state machine.
   *
   * @param name The command name.
   * @param args The command arguments.
   * @return The command output.
   */
  Object applyCommand(String name, JsonObject args);

  /**
   * Takes a snapshot of the state machine.
   *
   * @return The state machine state.
   */
  JsonElement takeSnapshot();

  /**
   * Installs a snapshot of the state machine.
   *
   * @param snapshot The state machine state.
   */
  void installSnapshot(JsonElement snapshot);

}
