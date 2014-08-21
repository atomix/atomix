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
package net.kuujo.copycat.log.impl;

import java.util.List;

import net.kuujo.copycat.log.Entry;

/**
 * State machine command log entry.<p>
 *
 * This log entry stores information about a state machine command
 * that was submitted to the cluster. When a command entry is committed
 * to a replica's log, the replica should immediately apply the command
 * to its state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends Entry {
  private static final long serialVersionUID = 3257184123473104135L;
  private String command;
  private List<Object> args;

  public CommandEntry() {
    super();
  }

  public CommandEntry(long term, String command, List<Object> args) {
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
  public List<Object> args() {
    return args;
  }

  @Override
  public String toString() {
    return String.format("CommandEntry[term=%d, command=%s, args=%s]", term(), command, args);
  }

}
