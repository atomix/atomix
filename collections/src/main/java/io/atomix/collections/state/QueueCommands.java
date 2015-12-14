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
package io.atomix.collections.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

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

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
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
  @SerializeWith(id=90)
  public static class Contains extends ValueQuery<Boolean> {
    public Contains() {
    }

    public Contains(Object value) {
      super(value);
    }
  }

  /**
   * Add command.
   */
  @SerializeWith(id=91)
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
  @SerializeWith(id=92)
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
  @SerializeWith(id=93)
  public static class Peek extends QueueQuery<Object> {
  }

  /**
   * Poll command.
   */
  @SerializeWith(id=94)
  public static class Poll extends QueueCommand<Object> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Element command.
   */
  @SerializeWith(id=95)
  public static class Element extends QueueCommand<Object> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=96)
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
  @SerializeWith(id=97)
  public static class Size extends QueueQuery<Integer> {
  }

  /**
   * Is empty query.
   */
  @SerializeWith(id=98)
  public static class IsEmpty extends QueueQuery<Boolean> {
  }

  /**
   * Clear command.
   */
  @SerializeWith(id=99)
  public static class Clear extends QueueCommand<Void> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

}
