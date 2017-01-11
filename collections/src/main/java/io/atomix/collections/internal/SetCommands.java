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
package io.atomix.collections.internal;

import java.util.Set;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.collections.DistributedSet;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

/**
 * Distributed set commands.
 * <p>
 * This class reserves serializable type IDs {@code 100} through {@code 109}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SetCommands {

  private SetCommands() {
  }

  /**
   * Abstract set command.
   */
  private static abstract class SetCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

  /**
   * Abstract set query.
   */
  private static abstract class SetQuery<V> implements Query<V>, CatalystSerializable {
    protected ConsistencyLevel consistency;

    protected SetQuery() {
    }

    protected SetQuery(ConsistencyLevel consistency) {
      this.consistency = consistency;
    }

    @Override
    public void writeObject(BufferOutput<?> output, Serializer serializer) {
      if (consistency != null) {
        output.writeByte(consistency.ordinal());
      } else {
        output.writeByte(-1);
      }
    }

    @Override
    public void readObject(BufferInput<?> input, Serializer serializer) {
      int ordinal = input.readByte();
      if (ordinal != -1) {
        consistency = ConsistencyLevel.values()[ordinal];
      }
    }
  }

  /**
   * Abstract value command.
   */
  private static abstract class ValueCommand<V> extends SetCommand<V> {
    protected Object value;

    public ValueCommand() {
    }

    public ValueCommand(Object value) {
      this.value = value;
    }

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
    }
  }

  /**
   * Abstract value query.
   */
  private static abstract class ValueQuery<V> extends SetQuery<V> {
    protected Object value;

    public ValueQuery() {
    }

    public ValueQuery(Object value) {
      this.value = value;
    }

    public ValueQuery(Object value, ConsistencyLevel consistency) {
      super(consistency);
      this.value = value;
    }

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }
  }

  /**
   * Contains value command.
   */
  public static class Contains extends ValueQuery<Boolean> {
    public Contains() {
    }

    public Contains(Object value) {
      super(value);
    }

    public Contains(Object value, ConsistencyLevel consistency) {
      super(value, consistency);
    }
  }

  /**
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends ValueCommand<V> {
    protected long ttl;

    protected TtlCommand() {
    }

    protected TtlCommand(Object value, long ttl) {
      super(value);
      this.ttl = ttl;
    }

    @Override
    public CompactionMode compaction() {
      return ttl > 0 ? CompactionMode.EXPIRING : CompactionMode.QUORUM;
    }

    /**
     * Returns the time to live in milliseconds.
     *
     * @return The time to live in milliseconds.
     */
    public long ttl() {
      return ttl;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(ttl);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      ttl = buffer.readLong();
    }
  }

  /**
   * Add command.
   */
  public static class Add extends TtlCommand<Boolean> {
    public Add() {
    }

    public Add(Object value) {
      super(value, 0);
    }

    public Add(Object value, long ttl) {
      super(value, ttl);
    }
  }

  /**
   * Remove command.
   */
  public static class Remove extends ValueCommand<Boolean> {

    public Remove() {
    }

    public Remove(Object value) {
      super(value);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Size query.
   */
  public static class Size extends SetQuery<Integer> {
    public Size() {
    }

    public Size(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Is empty query.
   */
  public static class IsEmpty extends SetQuery<Boolean> {
    public IsEmpty() {
    }

    public IsEmpty(ConsistencyLevel consistency) {
      super(consistency);
    }
  }
  
  /**
   * Iterator query.
   */
  public static class Iterator<V> extends SetQuery<Set<V>> {
  }

  /**
   * Clear command.
   */
  public static class Clear extends SetCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Set command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Contains.class, -100);
      registry.register(Add.class, -101);
      registry.register(Remove.class, -102);
      registry.register(IsEmpty.class, -103);
      registry.register(Size.class, -104);
      registry.register(Clear.class, -105);
      registry.register(Iterator.class, -106);
      registry.register(DistributedSet.ValueEvent.class, -48);
    }
  }

}
