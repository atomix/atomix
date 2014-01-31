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

import java.util.UUID;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.log.Log;

/**
 * A default command implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultCommand<T> implements Command<T> {
  @JsonIgnore
  private JsonObject command;
  @JsonIgnore
  private Log log;

  public DefaultCommand() {
  }

  @JsonCreator
  private DefaultCommand(String json) {
    command = new JsonObject(json);
  }

  public DefaultCommand(String command, Command.Type type, T data) {
    this(UUID.randomUUID().toString(), command, type, data);
  }

  public DefaultCommand(String id, String command, Command.Type type, T data) {
    this.command = new JsonObject().putString("id", id).putString("command", command)
        .putString("type", type.getName()).putValue("data", data);
  }

  @Override
  public String id() {
    return command.getString("id");
  }

  @Override
  public Command.Type type() {
    return command.getFieldNames().contains("type") ? Command.Type.parse(command.getString("type")) : null;
  }

  @Override
  public String command() {
    return command.getString("command");
  }

  @Override
  public T data() {
    return command.getValue("data");
  }

  /**
   * Returns command data.
   * 
   * @return The command data.
   */
  public Command<T> setLog(Log log) {
    this.log = log;
    return this;
  }

  @Override
  public void free() {
    if (log != null) {
      log.free(id());
    }
  }

  @JsonValue
  private String getJsonValue() {
    return command.encode();
  }

}
