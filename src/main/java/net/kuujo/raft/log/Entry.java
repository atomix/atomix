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
package net.kuujo.raft.log;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A log entry.
 * 
 * @author Jordan Halterman
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonAutoDetect(creatorVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = NoOpEntry.class, name = "no-op"),
    @JsonSubTypes.Type(value = ConfigurationEntry.class, name = "configuration"),
    @JsonSubTypes.Type(value = CommandEntry.class, name = "command") })
public abstract class Entry {

  /**
   * A log entry type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {
    NOOP("no-op"), CONFIGURATION("configuration"), COMMAND("command");

    private final String name;

    /**
     * Constructor.
     * 
     * @param name The string name of the entry type.
     */
    private Type(String name) {
      this.name = name;
    }

    /**
     * Returns the entry type name.
     * 
     * @return A string representation of the entry type.
     */
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return getName();
    }

    /**
     * Parses an entry type name to an entry type.
     *
     * @param name a string representation of the entry type.
     * @return an entry type.
     * @throws IllegalArgumentException if the entry type name is invalid.
     */
    public static Type parse(String name) {
      switch (name) {
        case "no-op":
          return NOOP;
        case "configuration":
          return CONFIGURATION;
        case "command":
          return COMMAND;
        default:
          throw new IllegalArgumentException("Invalid entry type " + name);
      }
    }
  }

  private Type type;
  private long term;

  /**
   * Constructor.
   */
  protected Entry() {
  }

  /**
   * Constructor.
   *
   * @param type The entry type.
   */
  protected Entry(Type type) {
    this.type = type;
  }

  /**
   * Constructor.
   *
   * @param type The entry type.
   * @param term The entry term.
   */
  protected Entry(Type type, long term) {
    this.type = type;
    this.term = term;
  }

  /**
   * Returns the entry type.
   * 
   * @return The entry type.
   */
  public Type type() {
    return type;
  }

  @JsonGetter("type")
  private String getSerializedType() {
    return type.getName();
  }

  @JsonSetter("type")
  private void setSerializedType(String name) {
    type = Type.parse(name);
  }

  /**
   * Returns the log entry term.
   * 
   * @return The log entry term.
   */
  public long term() {
    return term;
  }

}
