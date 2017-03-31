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
import io.atomix.collections.DistributedMap;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

import java.util.Collection;
import java.util.Set;

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
  public static abstract class MapQuery<V> implements Query<V>, CatalystSerializable {
    protected ConsistencyLevel consistency;

    protected MapQuery() {
    }

    protected MapQuery(ConsistencyLevel consistency) {
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

    public KeyQuery(Object key, ConsistencyLevel consistency) {
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
   * Contains key command.
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
   * Abstract key-based query.
   */
  public static class ContainsValue extends MapQuery<Boolean> {
    protected Object value;

    public ContainsValue() {
    }

    public ContainsValue(Object value) {
      this.value = Assert.notNull(value, "value");
    }

    public ContainsValue(Object value, ConsistencyLevel consistency) {
      super(consistency);
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
  public static class Get extends KeyQuery<Object> {
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
   * Get or default query.
   */
  public static class GetOrDefault extends KeyQuery<Object> {
    private Object defaultValue;

    public GetOrDefault() {
    }

    public GetOrDefault(Object key, Object defaultValue) {
      super(key);
      this.defaultValue = defaultValue;
    }

    public GetOrDefault(Object key, Object defaultValue, ConsistencyLevel consistency) {
      super(key, consistency);
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
  public static class Remove extends KeyCommand<Object> {

    public Remove() {
    }

    public Remove(Object key) {
      super(key);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Remove if absent command.
   */
  public static class RemoveIfPresent extends KeyValueCommand<Boolean> {

    public RemoveIfPresent() {
    }

    public RemoveIfPresent(Object key, Object value) {
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
  public static class ReplaceIfPresent extends TtlCommand<Boolean> {
    private Object replace;

    public ReplaceIfPresent() {
    }

    public ReplaceIfPresent(Object key, Object replace, Object value) {
      this(key, replace, value, 0);
    }

    public ReplaceIfPresent(Object key, Object replace, Object value, long ttl) {
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
  public static class IsEmpty extends MapQuery<Boolean> {
    public IsEmpty() {
    }

    public IsEmpty(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Size query.
   */
  public static class Size extends MapQuery<Integer> {
    public Size() {
    }

    public Size(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Values query.
   */
  public static class Values extends MapQuery<Collection> {
    public Values() {
    }

    public Values(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Key set query.
   */
  public static class KeySet extends MapQuery<Set> {
    public KeySet() {
    }

    public KeySet(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Entry set query.
   */
  public static class EntrySet extends MapQuery<Set> {
    public EntrySet() {
    }

    public EntrySet(ConsistencyLevel consistency) {
      super(consistency);
    }
  }

  /**
   * Clear command.
   */
  public static class Clear extends MapCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Map key listen command.
   */
  public static abstract class EventCommand extends MapCommand<Void> {
    private int event;
    private Object key;

    protected EventCommand() {
    }

    protected EventCommand(int event, Object key) {
      this.event = event;
      this.key = key;
    }

    /**
     * Returns the event type for which to listen.
     *
     * @return The event type for which to listen.
     */
    public int event() {
      return event;
    }

    /**
     * Returns the key to which to listen.
     *
     * @return The key to which to listen.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeByte(event);
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      event = buffer.readByte();
      key = serializer.readObject(buffer);
    }
  }

  /**
   * Map key listen command.
   */
  public static class KeyListen extends EventCommand {
    public KeyListen() {
    }

    public KeyListen(int event, Object key) {
      super(event, key);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Map key unlisten command.
   */
  public static class KeyUnlisten extends EventCommand {
    public KeyUnlisten() {
    }

    public KeyUnlisten(int event, Object key) {
      super(event, key);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.TOMBSTONE;
    }
  }

  /**
   * Map command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(ContainsKey.class, -65);
      registry.register(ContainsValue.class, -66);
      registry.register(Put.class, -67);
      registry.register(PutIfAbsent.class, -68);
      registry.register(Get.class, -69);
      registry.register(GetOrDefault.class, -70);
      registry.register(Remove.class, -71);
      registry.register(RemoveIfPresent.class, -72);
      registry.register(Replace.class, -73);
      registry.register(ReplaceIfPresent.class, -74);
      registry.register(Values.class, -155);
      registry.register(KeySet.class, -156);
      registry.register(EntrySet.class, -157);
      registry.register(IsEmpty.class, -75);
      registry.register(Size.class, -76);
      registry.register(Clear.class, -77);
      registry.register(DistributedMap.EntryEvent.class, -78);
      registry.register(KeyListen.class, -168);
      registry.register(KeyUnlisten.class, -169);
    }
  }

}
