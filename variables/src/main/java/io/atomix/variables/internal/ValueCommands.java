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
package io.atomix.variables.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

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
  public static class Get<T> extends ValueQuery<T> {
  }

  /**
   * Set command.
   */
  public static class Set<T> extends ValueCommand<T> {
    private T value;

    public Set() {
    }

    public Set(T value) {
      this.value = value;
    }

    public Set(T value, long ttl) {
      super(ttl);
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public T value() {
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
  public static class CompareAndSet<T> extends ValueCommand<Boolean> {
    private T expect;
    private T update;

    public CompareAndSet() {
    }

    public CompareAndSet(T expect, T update) {
      this.expect = expect;
      this.update = update;
    }

    public CompareAndSet(T expect, T update, long ttl) {
      super(ttl);
      this.expect = expect;
      this.update = update;
    }

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public T expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public T update() {
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
  public static class GetAndSet<T> extends ValueCommand<T> {
    private T value;

    public GetAndSet() {
    }

    public GetAndSet(T value) {
      this.value = value;
    }

    public GetAndSet(T value, long ttl) {
      super(ttl);
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public T value() {
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
   * Value command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(ValueCommands.CompareAndSet.class, -110);
      registry.register(ValueCommands.Get.class, -111);
      registry.register(ValueCommands.GetAndSet.class, -112);
      registry.register(ValueCommands.Set.class, -113);
    }
  }

}
