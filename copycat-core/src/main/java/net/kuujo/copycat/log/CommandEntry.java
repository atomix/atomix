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
package net.kuujo.copycat.log;

import java.util.Map;

/**
 * A state machine command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends Entry {
  private String command;
  private Map<String, Object> args;

  public CommandEntry() {
    super();
  }

  public CommandEntry(long term, String command, Map<String, Object> args) {
    super(term);
    this.command = command;
    this.args = args;
  }

  /**
   * Returns the state machine command.
   * 
   * @return The state machine command.
   */
  public String command() {
    return command;
  }

  /**
   * Returns command arguments.
   *
   * @return The command arguments.
   */
  public Map<String, Object> args() {
    return args;
  }

  @Override
  public String toString() {
    return String.format("Command[%s(%s)]", command, args);
  }

}
