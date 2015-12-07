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

/**
 * Map commands.
 * <p>
 * This class reserves serializable type IDs {@code 60} through {@code 74}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapCommands {

  private MapCommands() {
  }

  /**
   * Abstract map command.
   */
  public static abstract class MapCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM_CLEAN;
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

    public KeyCommand() {
    }

    public KeyCommand(Object key) {
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

    public KeyQuery() {
    }

    public KeyQuery(Object key) {
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
   * Contains key command.
   */
  @SerializeWith(id=60)
  public static class ContainsKey extends KeyQuery<Boolean> {
    public ContainsKey() {
    }

    public ContainsKey(Object key) {
      super(key);
    }
  }

  /**
   * Abstract key-based query.
   */
  @SerializeWith(id=61)
  public static class ContainsValue extends MapQuery<Boolean> {
    protected Object value;

    public ContainsValue() {
    }

    public ContainsValue(Object value) {
      this.value = Assert.notNull(value, "value");
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
   * Key/value command.
   */
  public static abstract class KeyValueCommand<V> extends KeyCommand<V> {
    protected Object value;

    public KeyValueCommand() {
    }

    public KeyValueCommand(Object key, Object value) {
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
  public static abstract class TtlCommand<V> extends KeyValueCommand<V> {
    protected long ttl;

    public TtlCommand() {
    }

    public TtlCommand(Object key, Object value, long ttl) {
      super(key, value);
      this.ttl = ttl;
    }

    @Override
    public CompactionMode compaction() {
      return ttl > 0 ? CompactionMode.FULL_SEQUENTIAL_CLEAN : CompactionMode.FULL_CLEAN;
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
  @SerializeWith(id=62)
  public static class Put extends TtlCommand<Object> {
    public Put() {
    }

    public Put(Object key, Object value) {
      this(key, value, 0);
    }

    public Put(Object key, Object value, long ttl) {
      super(key, value, ttl);
    }
  }

  /**
   * Put if absent command.
   */
  @SerializeWith(id=63)
  public static class PutIfAbsent extends TtlCommand<Object> {
    public PutIfAbsent() {
    }

    public PutIfAbsent(Object key, Object value) {
      this(key, value, 0);
    }

    public PutIfAbsent(Object key, Object value, long ttl) {
      super(key, value, ttl);
    }
  }

  /**
   * Get query.
   */
  @SerializeWith(id=64)
  public static class Get extends KeyQuery<Object> {
    public Get() {
    }

    public Get(Object key) {
      super(key);
    }
  }

  /**
   * Get or default query.
   */
  @SerializeWith(id=65)
  public static class GetOrDefault extends KeyQuery<Object> {
    private Object defaultValue;

    public GetOrDefault() {
    }

    public GetOrDefault(Object key, Object defaultValue) {
      super(key);
      this.defaultValue = defaultValue;
    }

    /**
     * Returns the default value.
     *
     * @return The default value.
     */
    public Object defaultValue() {
      return defaultValue;
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      defaultValue = serializer.readObject(buffer);
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(defaultValue, buffer);
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=66)
  public static class Remove extends KeyCommand<Object> {

    public Remove() {
    }

    public Remove(Object key) {
      super(key);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.FULL_SEQUENTIAL_COMMIT;
    }
  }

  /**
   * Remove if absent command.
   */
  @SerializeWith(id=67)
  public static class RemoveIfPresent extends KeyValueCommand<Boolean> {

    public RemoveIfPresent() {
    }

    public RemoveIfPresent(Object key, Object value) {
      super(key, value);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.FULL_SEQUENTIAL_COMMIT;
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=68)
  public static class Replace extends TtlCommand<Object> {
    public Replace() {
    }

    public Replace(Object key, Object value) {
      this(key, value, 0);
    }

    public Replace(Object key, Object value, long ttl) {
      super(key, value, ttl);
    }
  }

  /**
   * Remove if absent command.
   */
  @SerializeWith(id=69)
  public static class ReplaceIfPresent extends TtlCommand<Boolean> {
    private Object replace;

    public ReplaceIfPresent() {
    }

    public ReplaceIfPresent(Object key, Object value, Object replace) {
      this(key, value, replace, 0);
    }

    public ReplaceIfPresent(Object key, Object value, Object replace, long ttl) {
      super(key, value, ttl);
      this.replace = replace;
    }

    /**
     * Returns the replace value.
     *
     * @return The replace value.
     */
    public Object replace() {
      return replace;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(replace, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      replace = serializer.readObject(buffer);
    }
  }

  /**
   * Is empty query.
   */
  @SerializeWith(id=70)
  public static class IsEmpty extends MapQuery<Boolean> {
  }

  /**
   * Size query.
   */
  @SerializeWith(id=71)
  public static class Size extends MapQuery<Integer> {
  }

  /**
   * Clear command.
   */
  @SerializeWith(id=72)
  public static class Clear extends MapCommand<Void> {

    @Override
    public CompactionMode compaction() {
      return CompactionMode.FULL_SEQUENTIAL_COMMIT;
    }
  }

}
