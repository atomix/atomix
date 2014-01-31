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
package net.kuujo.copycat;

import net.kuujo.copycat.impl.DefaultCommand;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

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
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, property="class", defaultImpl=DefaultCommand.class)
public interface Command<T> {

  /**
   * A command type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {
    READ("read", true, false),
    WRITE("write", false, true),
    READ_WRITE("read-write", true, true);

    private final String name;
    private final boolean read;
    private final boolean write;

    private Type(String name, boolean read, boolean write) {
      this.name = name;
      this.read = read;
      this.write = write;
    }

    /**
     * Returns a string representation of the command type for serialization.
     * 
     * @return a string representation of the command type.
     */
    public String getName() {
      return name;
    }

    /**
     * Returns a boolean indicating whether the command type is read-only.
     * 
     * @return Indicates whether the command type is read-only.
     */
    public boolean isReadOnly() {
      return read && !write;
    }

    /**
     * Returns a boolean indicating whether the command type is write-only.
     * 
     * @return Indicates whether the command type is write-only.
     */
    public boolean isWriteOnly() {
      return write && !read;
    }

    @Override
    public String toString() {
      return getName();
    }

    /**
     * Parses a command type from string.
     *
     * @param name The command type name.
     * @return A command type.
     */
    public static Type parse(String name) {
      switch (name) {
        case "read":
          return READ;
        case "write":
          return WRITE;
        case "read-write":
          return READ_WRITE;
        default:
          throw new IllegalArgumentException("Invalid command type name " + name);
      }
    }
  }

  /**
   * Returns the command type.
   * 
   * @return The command type.
   */
  public Command.Type type();

  /**
   * Returns the command name.
   * 
   * @return The command name.
   */
  public String command();

  /**
   * Returns the command data.
   * 
   * @return The command data.
   */
  public T data();

  /**
   * Frees the command from the log.
   */
  public void free();

}
