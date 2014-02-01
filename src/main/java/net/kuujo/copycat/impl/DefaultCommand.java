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
package net.kuujo.copycat.impl;

import java.util.List;
import java.util.Map;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonIgnore;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.log.Entry;

/**
 * A default command implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultCommand<T> implements Command<T> {
  private String command;
  private Type type;
  private Object data;
  @JsonIgnore
  private Entry entry;

  public DefaultCommand() {
  }

  public DefaultCommand(String command, T data) {
    this(command, null, data);
  }

  public DefaultCommand(String command, Command.Type type, T data) {
    this.command = command;
    this.type = type;
    if (data instanceof JsonObject) {
      this.data = ((JsonObject) data).toMap();
    }
    else if (data instanceof JsonArray) {
      this.data = ((JsonArray) data).toArray();
    }
    else {
      this.data = (T) data;
    }
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
  @SuppressWarnings("unchecked")
  public T data() {
    if (data instanceof Map) {
      return (T) new JsonObject((Map<String, Object>) data);
    }
    else if (data instanceof List) {
      return (T) new JsonArray((List<Object>) data);
    }
    return (T) data;
  }

  public DefaultCommand<T> setEntry(Entry entry) {
    this.entry = entry;
    return this;
  }

  @Override
  public void free() {
    if (entry != null) {
      entry.free();
    }
  }

}
