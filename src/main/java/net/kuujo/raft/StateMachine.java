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
package net.kuujo.raft;

import org.vertx.java.core.json.JsonObject;

/**
 * A replication service state machine.
 *
 * @author Jordan Halterman
 */
public interface StateMachine {

  /**
   * Registers a state machine command.
   *
   * @param command
   *   The command name.
   * @param type
   *   The command type.
   * @param function
   *   The command function.
   * @return
   *   The state machine.
   */
  StateMachine registerCommand(String command, Command.Type type, Function<Command, JsonObject> function);

  /**
   * Unregisters a state machine command.
   *
   * @param command
   *   The command name.
   * @return
   *   The state machine.
   */
  StateMachine unregisterCommand(String command);

  /**
   * Returns a boolean indicating whether the state machine has a command.
   *
   * @param command
   *   The command name.
   * @return
   *   Indicates whether the state machine has a command.
   */
  boolean hasCommand(String command);

  /**
   * Returns the type of a given command.
   *
   * @param command
   *   The command name.
   * @return
   *   The command type.
   */
  Command.Type getCommandType(String command);

  /**
   * Applies a command to the state machine.
   *
   * @param command
   *   The command to apply.
   * @return
   *   The command output.
   */
  JsonObject applyCommand(Command command);

}
