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
package net.kuujo.copycat;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;

/**
 * Copycat event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=2000)
public class Event implements Writable {

  private Type type;
  private String path;

  public Event() {
  }

  public Event(Type type, String path) {
    this.type = type;
    this.path = path;
  }

  /**
   * Returns the event type.
   *
   * @return The event type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the event path.
   *
   * @return The event path.
   */
  public String path() {
    return path;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(type.ordinal()).writeUTF8(path);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    type = Type.values()[buffer.readByte()];
    path = buffer.readUTF8();
  }

  /**
   * Event type.
   */
  public static enum Type {

    /**
     * Create path event.
     */
    CREATE_PATH,

    /**
     * Create resource event.
     */
    CREATE_RESOURCE,

    /**
     * State change event.
     */
    STATE_CHANGE,

    /**
     * Delete path event.
     */
    DELETE_PATH,

    /**
     * Delete resource event.
     */
    DELETE_RESOURCE

  }

}
