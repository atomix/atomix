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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.collections.DistributedQueue;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

/**
 * Distributed queue commands.
 * <p>
 * This class reserves serializable type IDs {@code 90} through {@code 99}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueueCommands {

  private QueueCommands() {
  }

  /**
   * Abstract queue command.
   */
  private static abstract class QueueCommand<V> implements Command<V>, CatalystSerializable {
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
   * Abstract queue query.
   */
  private static abstract class QueueQuery<V> implements Query<V>, CatalystSerializable {
    protected ConsistencyLevel consistency;

    protected QueueQuery() {
    }

    protected QueueQuery(ConsistencyLevel consistency) {
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
  public static abstract class ValueCommand<V> extends QueueCommand<V> {
    protected Object value;

    protected ValueCommand() {
    }

    protected ValueCommand(Object value) {
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
  public static abstract class ValueQuery<V> extends QueueQuery<V> {
    protected Object value;

    protected ValueQuery() {
    }

    protected ValueQuery(Object value) {
      this.value = value;
    }

    protected ValueQuery(Object value, ConsistencyLevel consistency) {
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
   * Add command.
   */
  public static class Add extends ValueCommand<Boolean> {
    public Add() {
    }

    public Add(Object value) {
      super(value);
    }
  }

  /**
   * Offer
   */
  public static class Offer extends ValueCommand<Boolean> {
    public Offer() {
    }

    public Offer(Object value) {
      super(value);
    }
  }

  /**
   * Peek query.
   */
  public static class Peek extends QueueQuery<Object> {
  }

  /**
   * Poll command.
   */
  public static class Poll extends QueueCommand<Object> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Element command.
   */
  public static class Element extends QueueCommand<Object> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Remove command.
   */
  public static class Remove extends ValueCommand<Object> {
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
  public static class Size extends QueueQuery<Integer> {
    public Size() {
    }

    public Size(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Is empty query.
   */
  public static class IsEmpty extends QueueQuery<Boolean> {
    public IsEmpty() {
    }

    public IsEmpty(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Clear command.
   */
  public static class Clear extends QueueCommand<Void> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Queue command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Contains.class, -90);
      registry.register(Add.class, -91);
      registry.register(Offer.class, -92);
      registry.register(Peek.class, -93);
      registry.register(Poll.class, -94);
      registry.register(Element.class, -95);
      registry.register(Remove.class, -96);
      registry.register(IsEmpty.class, -97);
      registry.register(Size.class, -98);
      registry.register(Clear.class, -99);
      registry.register(DistributedQueue.ValueEvent.class, -79);
    }
  }

}
