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

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.json.JsonObject;

/**
 * Command registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandRegistry {
  private final Map<String, CommandHolder> commands = new HashMap<>();
  private SnapshotCreator snapshotCreator;
  private SnapshotInstaller snapshotInstaller;

  /**
   * Returns a registered executable command.
   *
   * @param name The name of the command to return.
   * @return The registered command.
   */
  public Command<JsonObject, JsonObject> getCommand(String name) {
    CommandHolder command = commands.get(name);
    if (command != null) {
      return command.command;
    }
    return null;
  }

  /**
   * Returns command information for a registered command.
   *
   * @param name The name of the command for which to return info.
   * @return The registered command info.
   */
  public CommandInfo getCommandInfo(String name) {
    CommandHolder command = commands.get(name);
    if (command != null) {
      return command.info;
    }
    return null;
  }

  /**
   * Registers a command.
   *
   * @param name The name of the command to register.
   * @param command The command to register.
   * @return The command registry.
   */
  public CommandRegistry registerCommand(String name, Command<JsonObject, JsonObject> command) {
    this.commands.put(name, new CommandHolder(name, command, new GenericCommandInfo(name, CommandInfo.Type.READ_WRITE)));
    return this;
  }

  /**
   * Registers a command.
   *
   * @param name The name of the command to register.
   * @param type The type of the command to register.
   * @param command The command to register.
   * @return The command registry.
   */
  public CommandRegistry registerCommand(String name, CommandInfo.Type type, Command<JsonObject, JsonObject> command) {
    this.commands.put(name, new CommandHolder(name, command, new GenericCommandInfo(name, type)));
    return this;
  }

  /**
   * Unregisters a command.
   *
   * @param name The name of the command to unregister.
   * @return The command registry.
   */
  public CommandRegistry unregisterCommand(String name) {
    this.commands.remove(name);
    return this;
  }

  /**
   * Registers a snapshot creator.
   *
   * @param provider The snapshot creator.
   * @return The command registry.
   */
  public CommandRegistry snapshotCreator(SnapshotCreator creator) {
    this.snapshotCreator = creator;
    return this;
  }

  /**
   * Returns the registered snapshot provider.
   *
   * @return The registered snapshot provider.
   */
  public SnapshotCreator snapshotCreator() {
    return snapshotCreator;
  }

  /**
   * Registers a snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The command registry.
   */
  public CommandRegistry snapshotInstaller(SnapshotInstaller installer) {
    this.snapshotInstaller = installer;
    return this;
  }

  /**
   * Returns the registered snapshot provider.
   *
   * @return The registered snapshot provider.
   */
  public SnapshotInstaller snapshotInstaller() {
    return snapshotInstaller;
  }

  // Holder for commands and their related information.
  private static class CommandHolder {
    private final Command<JsonObject, JsonObject> command;
    private final CommandInfo info;

    public CommandHolder(String name, Command<JsonObject, JsonObject> command, CommandInfo info) {
      this.command = command;
      this.info = info;
    }
  }

}
