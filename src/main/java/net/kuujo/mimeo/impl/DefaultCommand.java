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
package net.kuujo.mimeo.impl;

import java.util.Map;
import java.util.UUID;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.log.Log;


/**
 * A default command implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultCommand implements Command {
  private String id;
  private Command.Type type;
  private String command;
  private Map<String, Object> data;
  @JsonIgnore
  private Log log;

  public DefaultCommand() {
  }

  public DefaultCommand(String command, Command.Type type, JsonObject data) {
    this(UUID.randomUUID().toString(), command, type, data);
  }

  public DefaultCommand(String id, String command, Command.Type type, JsonObject data) {
    this.id = id;
    this.type = type;
    this.command = command;
    this.data = data.toMap();
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Command.Type type() {
    return type;
  }

  @Override
  public String command() {
    return command;
  }

  @Override
  public JsonObject data() {
    return new JsonObject(data);
  }

  /**
   * Returns command data.
   *
   * @return
   *   The command data.
   */
  public Command setLog(Log log) {
    this.log = log;
    return this;
  }

  @Override
  public void free() {
    if (log != null) {
      log.free(id);
    }
  }

}
