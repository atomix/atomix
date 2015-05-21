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

import java.util.HashMap;
import java.util.Map;

/**
 * Resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Command<T> implements Writable {
  private static final ThreadLocal<Map<Class<? extends Builder>, Builder>> BUILDERS = new ThreadLocal<Map<Class<? extends Builder>, Builder>>() {
    @Override
    protected Map<Class<? extends Builder>, Builder> initialValue() {
      return new HashMap<>();
    }
  };

  /**
   * Returns a cached instance of the given builder.
   *
   * @param type The builder type.
   * @return The builder.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Builder> T builder(Class<T> type) {
    return (T) BUILDERS.get().computeIfAbsent(type, t -> {
      try {
        return type.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("failed to instantiate builder: " + type, e);
      }
    });
  }

  protected Persistence persistence = Persistence.DEFAULT;
  protected Consistency consistency = Consistency.DEFAULT;
  long resourceId;

  protected Command() {
  }

  protected Command(long resourceId) {
    this.resourceId = resourceId;
  }

  /**
   * Returns the resource ID.
   *
   * @return The resource ID.
   */
  public long resource() {
    return resourceId;
  }

  /**
   * Returns the command persistence level.
   *
   * @return The command persistence level.
   */
  public Persistence persistence() {
    return persistence;
  }

  /**
   * Returns the command consistency level.
   *
   * @return The command consistency level.
   */
  public Consistency consistency() {
    return consistency;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(persistence().ordinal())
      .writeByte(consistency().ordinal())
      .writeLong(resourceId);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    persistence = Persistence.values()[buffer.readByte()];
    consistency = Consistency.values()[buffer.readByte()];
    resourceId = buffer.readLong();
  }

  /**
   * Command builder.
   */
  public static class Builder<T extends Builder<T, U>, U extends Command<?>> implements net.kuujo.copycat.Builder<U> {
    protected final U command;

    protected Builder(U command) {
      this.command = command;
    }

    /**
     * Sets the command persistence level.
     *
     * @param persistence The command persistence level.
     * @return The command builder.
     */
    @SuppressWarnings("unchecked")
    public T withPersistence(Persistence persistence) {
      command.persistence = persistence;
      return (T) this;
    }

    /**
     * Sets the command consistency level.
     *
     * @param consistency The command consistency level.
     * @return The command builder.
     */
    @SuppressWarnings("unchecked")
    public T withConsistency(Consistency consistency) {
      command.consistency = consistency;
      return (T) this;
    }

    @Override
    public U build() {
      return command;
    }
  }

}
