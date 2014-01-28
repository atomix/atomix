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

import java.util.Map;
import java.util.UUID;

import net.kuujo.raft.log.Log;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A state machine command.
 *
 * @author Jordan Halterman
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonAutoDetect(
  creatorVisibility=JsonAutoDetect.Visibility.NONE,
  fieldVisibility=JsonAutoDetect.Visibility.ANY,
  getterVisibility=JsonAutoDetect.Visibility.NONE,
  isGetterVisibility=JsonAutoDetect.Visibility.NONE,
  setterVisibility=JsonAutoDetect.Visibility.NONE
)
public class Command {

  /**
   * A command type.
   *
   * @author Jordan Halterman
   */
  public static enum Type {
    READ("read"),
    WRITE("write"),
    READ_WRITE("read-write");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  private String id;
  private String command;
  private Map<String, Object> data;
  @JsonIgnore
  private Log log;

  public Command() {
  }

  public Command(String command, JsonObject data) {
    this(UUID.randomUUID().toString(), command, data);
  }

  public Command(String id, String command, JsonObject data) {
    this.id = id;
    this.command = command;
    this.data = data.toMap();
  }

  /**
   * Returns the command ID.
   *
   * @return
   *   The command ID.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the command name.
   *
   * @return
   *   The command name.
   */
  public String command() {
    return command;
  }

  /**
   * Returns the command data.
   *
   * @return
   *   The command data.
   */
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

  /**
   * Frees the command from the log.
   */
  public void free() {
    if (log != null) {
      log.free(id);
    }
  }

}
