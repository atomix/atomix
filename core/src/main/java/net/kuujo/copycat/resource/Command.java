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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;

/**
 * Resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Command<T> implements Writable {
  private Persistence persistence = Persistence.DEFAULT;
  private Consistency consistency = Consistency.DEFAULT;
  private long resourceId;

  protected Command() {
  }

  protected Command(long resourceId) {
    this.resourceId = resourceId;
  }

  /**
   * Sets the resource ID.
   *
   * @param resourceId The resource ID.
   * @return The command.
   */
  Command setResource(long resourceId) {
    this.resourceId = resourceId;
    return this;
  }

  /**
   * Returns the resource ID.
   *
   * @return The resource ID.
   */
  public long getResource() {
    return resourceId;
  }

  /**
   * Sets the command persistence level.
   *
   * @param persistence The command persistence level.
   * @return The command.
   */
  public Command setPersistence(Persistence persistence) {
    this.persistence = persistence;
    return this;
  }

  /**
   * Returns the command persistence level.
   *
   * @return The command persistence level.
   */
  public Persistence getPersistence() {
    return persistence;
  }

  /**
   * Sets the command consistency level.
   *
   * @param consistency The command consistency level.
   * @return The command.
   */
  public Command setConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Returns the command consistency level.
   *
   * @return The command consistency level.
   */
  public Consistency getConsistency() {
    return consistency;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(getPersistence().ordinal())
      .writeByte(getConsistency().ordinal())
      .writeLong(resourceId);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    persistence = Persistence.values()[buffer.readByte()];
    consistency = Consistency.values()[buffer.readByte()];
    resourceId = buffer.readLong();
  }

}
