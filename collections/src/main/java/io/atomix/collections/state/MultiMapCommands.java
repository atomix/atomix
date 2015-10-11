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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

import java.util.Collection;

/**
 * Map commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MultiMapCommands {

  private MultiMapCommands() {
  }

  /**
   * Abstract map command.
   */
  public static abstract class MapCommand<V> implements Command<V>, CatalystSerializable {

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
  public static abstract class MapQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

  /**
   * Abstract key-based command.
   */
  public static abstract class KeyCommand<V> extends MapCommand<V> {
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
  public static abstract class KeyQuery<V> extends MapQuery<V> {
    protected Object key;

    protected KeyQuery() {
    }

    protected KeyQuery(Object key) {
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
  public static abstract class ValueQuery<V> extends MapQuery<V> {
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
  @SerializeWith(id=460)
  public static class ContainsKey extends KeyQuery<Boolean> {
    public ContainsKey() {
    }

    public ContainsKey(Object key) {
      super(key);
    }
  }

  /**
   * Contains entry query.
   */
  @SerializeWith(id=461)
  public static class ContainsEntry extends EntryQuery<Boolean> {
    public ContainsEntry() {
    }

    public ContainsEntry(Object key, Object value) {
      super(key, value);
    }
  }

  /**
   * Contains value query.
   */
  @SerializeWith(id=462)
  public static class ContainsValue extends ValueQuery<Boolean> {
    public ContainsValue() {
    }

    public ContainsValue(Object value) {
      super(value);
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
    public PersistenceLevel persistence() {
      return ttl > 0 ? PersistenceLevel.PERSISTENT : PersistenceLevel.EPHEMERAL;
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
  @SerializeWith(id=463)
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
  @SerializeWith(id=464)
  public static class Get extends KeyQuery<Collection> {
    public Get() {
    }

    public Get(Object key) {
      super(key);
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=465)
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
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }

  /**
   * Is empty query.
   */
  @SerializeWith(id=466)
  public static class IsEmpty extends MapQuery<Boolean> {
  }

  /**
   * Size query.
   */
  @SerializeWith(id=467)
  public static class Size extends KeyQuery<Integer> {
    public Size() {
    }

    public Size(Object key) {
      super(key);
    }
  }

  /**
   * Clear command.
   */
  @SerializeWith(id=468)
  public static class Clear extends MapCommand<Void> {

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }

}
