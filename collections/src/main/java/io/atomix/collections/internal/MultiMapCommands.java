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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.util.Collection;

/**
 * Map commands.
 * <p>
 * This class reserves serializable type IDs {@code 75} through {@code 84}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MultiMapCommands {

  private MultiMapCommands() {
  }

  /**
   * Abstract map command.
   */
  public static abstract class MultiMapCommand<V> implements Command<V>, CatalystSerializable {
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
   * Abstract map query.
   */
  public static abstract class MultiMapQuery<V> implements Query<V>, CatalystSerializable {
    protected ConsistencyLevel consistency;

    protected MultiMapQuery() {
    }

    protected MultiMapQuery(ConsistencyLevel consistency) {
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
   * Abstract key-based command.
   */
  public static abstract class KeyCommand<V> extends MultiMapCommand<V> {
    protected Object key;

    protected KeyCommand() {
    }

    protected KeyCommand(Object key) {
      this.key = Assert.notNull(key, "key");
    }

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      key = serializer.readObject(buffer);
    }
  }

  /**
   * Abstract key-based query.
   */
  public static abstract class KeyQuery<V> extends MultiMapQuery<V> {
    protected Object key;

    protected KeyQuery() {
    }

    protected KeyQuery(Object key) {
      this.key = Assert.notNull(key, "key");
    }

    protected KeyQuery(Object key, ConsistencyLevel consistency) {
      super(consistency);
      this.key = Assert.notNull(key, "key");
    }

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      key = serializer.readObject(buffer);
    }
  }

  /**
   * Abstract value-based query.
   */
  public static abstract class ValueQuery<V> extends MultiMapQuery<V> {
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
   * Entry query.
   */
  public static class EntryQuery<V> extends KeyQuery<V> {
    protected Object value;

    protected EntryQuery() {
    }

    protected EntryQuery(Object key, Object value) {
      super(key);
      this.value = value;
    }

    protected EntryQuery(Object key, Object value, ConsistencyLevel consistency) {
      super(key, consistency);
      this.value = value;
    }

    /**
     * Returns the value.
     *
     * @return The value.
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
   * Contains key query.
   */
  public static class ContainsKey extends KeyQuery<Boolean> {
    public ContainsKey() {
    }

    public ContainsKey(Object key) {
      super(key);
    }

    public ContainsKey(Object key, ConsistencyLevel consistency) {
      super(key, consistency);
    }
  }

  /**
   * Contains entry query.
   */
  public static class ContainsEntry extends EntryQuery<Boolean> {
    public ContainsEntry() {
    }

    public ContainsEntry(Object key, Object value) {
      super(key, value);
    }

    public ContainsEntry(Object key, Object value, ConsistencyLevel consistency) {
      super(key, value, consistency);
    }
  }

  /**
   * Contains value query.
   */
  public static class ContainsValue extends ValueQuery<Boolean> {
    public ContainsValue() {
    }

    public ContainsValue(Object value) {
      super(value);
    }

    public ContainsValue(Object value, ConsistencyLevel consistency) {
      super(value, consistency);
    }
  }

  /**
   * Entry command.
   */
  public static abstract class EntryCommand<V> extends KeyCommand<V> {
    protected Object value;

    protected EntryCommand() {
    }

    protected EntryCommand(Object key) {
      super(key);
    }

    protected EntryCommand(Object key, Object value) {
      super(key);
      this.value = value;
    }

    /**
     * Returns the command value.
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
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends EntryCommand<V> {
    protected long ttl;

    public TtlCommand() {
    }

    public TtlCommand(Object key, Object value, long ttl) {
      super(key, value);
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
   * Put command.
   */
  public static class Put extends TtlCommand<Boolean> {
    public Put() {
    }

    public Put(Object key, Object value) {
      super(key, value, 0);
    }

    public Put(Object key, Object value, long ttl) {
      super(key, value, ttl);
    }
  }

  /**
   * Get query.
   */
  public static class Get extends KeyQuery<Collection> {
    public Get() {
    }

    public Get(Object key) {
      super(key);
    }

    public Get(Object key, ConsistencyLevel consistency) {
      super(key, consistency);
    }
  }

  /**
   * Remove command.
   */
  public static class Remove extends EntryCommand<Object> {
    public Remove() {
    }

    public Remove(Object key) {
      super(key);
    }

    public Remove(Object key, Object value) {
      super(key, value);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Remove command.
   */
  public static class RemoveValue extends MultiMapCommand<Void> {
    private Object value;

    public RemoveValue() {
    }

    public RemoveValue(Object value) {
      this.value = value;
    }

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
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
   * Is empty query.
   */
  public static class IsEmpty extends MultiMapQuery<Boolean> {
    public IsEmpty() {
    }

    public IsEmpty(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Size query.
   */
  public static class Size extends KeyQuery<Integer> {
    public Size() {
    }

    public Size(Object key) {
      super(key);
    }

    public Size(Object key, ConsistencyLevel consistency) {
      super(key, consistency);
    }
  }

  /**
   * Clear command.
   */
  public static class Clear extends MultiMapCommand<Void> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Multi-map command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(ContainsKey.class, -80);
      registry.register(ContainsEntry.class, -81);
      registry.register(ContainsValue.class, -82);
      registry.register(Put.class, -83);
      registry.register(Get.class, -84);
      registry.register(Remove.class, -85);
      registry.register(RemoveValue.class, -86);
      registry.register(IsEmpty.class, -87);
      registry.register(Size.class, -88);
      registry.register(Clear.class, -89);
    }
  }

}
