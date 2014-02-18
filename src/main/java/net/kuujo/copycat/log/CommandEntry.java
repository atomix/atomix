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

import org.vertx.java.core.json.JsonObject;

/**
 * A state machine command entry.
 * 
 * @author Jordan Halterman
 */
public class CommandEntry extends Entry {
  private String command;
  private Map<String, Object> args;

  public CommandEntry() {
    super();
  }

  public CommandEntry(long term, String command, JsonObject args) {
    super(term);
    this.command = command;
    this.args = args.toMap();
  }

  @Override
  public Type type() {
    return Type.COMMAND;
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
  public JsonObject args() {
    return new JsonObject(args);
  }

}
