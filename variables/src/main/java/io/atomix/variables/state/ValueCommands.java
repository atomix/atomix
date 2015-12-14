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
package io.atomix.variables.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

/**
 * Distributed value commands.
 * <p>
 * This class reserves serializable type IDs {@code 50} through {@code 59}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ValueCommands {

  private ValueCommands() {
  }

  /**
   * Abstract value command.
   */
  public static abstract class ValueCommand<V> implements Command<V>, CatalystSerializable {
    protected long ttl;

    protected ValueCommand() {
    }

    protected ValueCommand(long ttl) {
      this.ttl = ttl;
    }

    @Override
    public CompactionMode compaction() {
      return ttl > 0 ? CompactionMode.SEQUENTIAL : CompactionMode.QUORUM;
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
      buffer.writeLong(ttl);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      ttl = buffer.readLong();
    }
  }

  /**
   * Abstract value query.
   */
  public static abstract class ValueQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> bufferInput, Serializer serializer) {
    }
  }

  /**
   * Get query.
   */
  @SerializeWith(id=50)
  public static class Get<T> extends ValueQuery<T> {
  }

  /**
   * Set command.
   */
  @SerializeWith(id=51)
  public static class Set extends ValueCommand<Void> {
    private Object value;

    public Set() {
    }

    public Set(Object value) {
      this.value = value;
    }

    public Set(Object value, long ttl) {
      super(ttl);
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
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

    @Override
    public String toString() {
      return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }
  }

  /**
   * Compare and set command.
   */
  @SerializeWith(id=52)
  public static class CompareAndSet extends ValueCommand<Boolean> {
    private Object expect;
    private Object update;

    public CompareAndSet() {
    }

    public CompareAndSet(Object expect, Object update) {
      this.expect = expect;
      this.update = update;
    }

    public CompareAndSet(Object expect, Object update, long ttl) {
      super(ttl);
      this.expect = expect;
      this.update = update;
    }

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public Object expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public Object update() {
      return update;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(expect, buffer);
      serializer.writeObject(update, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      expect = serializer.readObject(buffer);
      update = serializer.readObject(buffer);
    }

    @Override
    public String toString() {
      return String.format("%s[expect=%s, update=%s]", getClass().getSimpleName(), expect, update);
    }
  }

  /**
   * Get and set command.
   */
  @SerializeWith(id=53)
  public static class GetAndSet<T> extends ValueCommand<T> {
    private Object value;

    public GetAndSet() {
    }

    public GetAndSet(Object value) {
      this.value = value;
    }

    public GetAndSet(Object value, long ttl) {
      super(ttl);
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
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

    @Override
    public String toString() {
      return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }
  }

  /**
   * Change listen.
   */
  @SerializeWith(id=54)
  public static class Listen implements Command<Void>, CatalystSerializable {
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
   * Change unlisten.
   */
  @SerializeWith(id=55)
  public static class Unlisten implements Command<Void>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

}
