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
package net.kuujo.copycat.state;

import net.kuujo.copycat.protocol.Consistency;

/**
 * Command info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandInfo {
  private final String name;
  private final Command.Type type;
  private final Command command;
  private final Consistency consistency;

  CommandInfo(String name, Command.Type type, Command command, Consistency consistency) {
    this.name = name;
    this.type = type;
    this.command = command;
    this.consistency = consistency;
  }

  /**
   * Returns the command name.
   *
   * @return The command name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the command type.
   *
   * @return The command type.
   */
  public Command.Type type() {
    return type;
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command command() {
    return command;
  }

  /**
   * Returns the command consistency.
   *
   * @return The command consistency.
   */
  public Consistency consistency() {
    return consistency;
  }

}
