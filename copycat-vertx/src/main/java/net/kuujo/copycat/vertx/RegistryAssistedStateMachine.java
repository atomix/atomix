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
package net.kuujo.copycat.vertx;

import net.kuujo.copycat.CommandInfo;
import net.kuujo.copycat.CommandProvider;
import net.kuujo.copycat.StateMachine;

import org.vertx.java.core.json.JsonObject;

/**
 * State machine implementation that executes commands from a command registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegistryAssistedStateMachine implements StateMachine, CommandProvider, SnapshotCreator, SnapshotInstaller {
  private final CommandRegistry registry;

  public RegistryAssistedStateMachine(CommandRegistry registry) {
    this.registry = registry;
  }

  @Override
  public CommandInfo getCommandInfo(String name) {
    return registry.getCommandInfo(name);
  }

  @Override
  public JsonObject applyCommand(String name, JsonObject args) {
    Command<JsonObject, JsonObject> command = registry.getCommand(name);
    if (command != null) {
      return command.execute(args);
    }
    return null;
  }

  @Override
  public JsonObject createSnapshot() {
    SnapshotCreator provider = registry.snapshotCreator();
    if (provider != null) {
      return provider.createSnapshot();
    }
    return null;
  }

  @Override
  public void installSnapshot(JsonObject snapshot) {
    SnapshotInstaller installer = registry.snapshotInstaller();
    if (installer != null) {
      installer.installSnapshot(snapshot);
    }
  }

}
