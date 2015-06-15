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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.AbstractResource;
import net.kuujo.copycat.Mode;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.Compaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous map.
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(AsyncMap.StateMachine.class)
public class AsyncMap<K, V> extends AbstractResource {

  public AsyncMap(Protocol protocol) {
    super(protocol);
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(IsEmpty.builder().build());
  }

  /**
   * Checks whether the map is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
    return submit(IsEmpty.builder().withConsistency(consistency).build());
  }

  /**
   * Gets the size of the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(Size.builder().build());
  }

  /**
   * Gets the size of the map.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
    return submit(Size.builder().withConsistency(consistency).build());
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(Object key) {
    return submit(ContainsKey.builder()
      .withKey(key)
      .build());
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(Object key, ConsistencyLevel consistency) {
    return submit(ContainsKey.builder()
      .withKey(key)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key) {
    return submit(Get.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key, ConsistencyLevel consistency) {
    return submit(Get.builder()
      .withKey(key)
      .withConsistency(consistency)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param mode The mode in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, Mode mode) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .withMode(mode)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param mode The mode in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, Mode mode) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .withMode(mode)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param unit The time to live unit.
   * @param mode The mode in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit, Mode mode) {
    return submit(Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .withMode(mode)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> remove(Object key) {
    return submit(Remove.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key          The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return submit(GetOrDefault.builder()
      .withKey(key)
      .withDefaultValue(defaultValue)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key          The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue, ConsistencyLevel consistency) {
    return submit(GetOrDefault.builder()
      .withKey(key)
      .withDefaultValue(defaultValue)
      .withConsistency(consistency)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return submit(PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl) {
    return submit(PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live in milliseconds.
   * @param mode The mode in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, Mode mode) {
    return submit(PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .withMode(mode)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit) {
    return submit(PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @param mode The mode in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit, Mode mode) {
    return submit(PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .withMode(mode)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit(Remove.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (boolean) result);
  }

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(Clear.builder().build());
  }

  /**
   * Abstract map command.
   */
  public static abstract class MapCommand<V> implements Command<V>, Writable {

    /**
     * Base map command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends MapCommand<?>> extends Command.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }
    }
  }

  /**
   * Abstract map query.
   */
  public static abstract class MapQuery<V> implements Query<V>, Writable {
    protected ConsistencyLevel consistency = ConsistencyLevel.LINEARIZABLE_LEASE;

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      buffer.writeByte(consistency.ordinal());
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      consistency = ConsistencyLevel.values()[buffer.readByte()];
    }

    /**
     * Base map query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends MapQuery<?>> extends Query.Builder<T, U> {
      protected Builder(U query) {
        super(query);
      }

      /**
       * Sets the query consistency level.
       *
       * @param consistency The query consistency level.
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withConsistency(ConsistencyLevel consistency) {
        if (consistency == null)
          throw new NullPointerException("consistency cannot be null");
        query.consistency = consistency;
        return (T) this;
      }

    }
  }

  /**
   * Abstract key-based command.
   */
  public static abstract class KeyCommand<V> extends MapCommand<V> {
    protected Object key;

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      key = serializer.readObject(buffer);
    }

    /**
     * Base key command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends KeyCommand<?>> extends MapCommand.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }

      /**
       * Sets the command key.
       *
       * @param key The command key
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withKey(Object key) {
        command.key = key;
        return (T) this;
      }
    }
  }

  /**
   * Abstract key-based query.
   */
  public static abstract class KeyQuery<V> extends MapQuery<V> {
    protected Object key;

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      key = serializer.readObject(buffer);
    }

    /**
     * Base key query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends KeyQuery<?>> extends MapQuery.Builder<T, U> {
      protected Builder(U query) {
        super(query);
      }

      /**
       * Sets the query key.
       *
       * @param key The query key
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withKey(Object key) {
        query.key = key;
        return (T) this;
      }
    }
  }

  /**
   * Contains key command.
   */
  public static class ContainsKey extends KeyQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, ContainsKey> {
      public Builder() {
        super(new ContainsKey());
      }
    }
  }

  /**
   * Key/value command.
   */
  public static class KeyValueCommand<V> extends KeyCommand<V> {
    protected Object value;

    /**
     * Returns the command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }

    /**
     * Key/value command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends KeyValueCommand<?>> extends KeyCommand.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withValue(Object value) {
        command.value = value;
        return (T) this;
      }
    }
  }

  /**
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends KeyValueCommand<V> {
    protected Mode mode = Mode.PERSISTENT;
    protected long ttl;

    /**
     * Returns the persistence mode.
     *
     * @return The persistence mode.
     */
    public Mode mode() {
      return mode;
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
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeByte(mode.ordinal()).writeLong(ttl);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      mode = Mode.values()[buffer.readByte()];
      ttl = buffer.readLong();
    }

    /**
     * TTL command builder.
     */
    public static class Builder<T extends Builder<T, U>, U extends TtlCommand<?>> extends KeyValueCommand.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }

      /**
       * Sets the persistence mode.
       *
       * @param mode The persistence mode.
       * @return The command builder.
       */
      public Builder withMode(Mode mode) {
        command.mode = mode;
        return this;
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live in milliseconds..
       * @return The command builder.
       */
      public Builder withTtl(long ttl) {
        command.ttl = ttl;
        return this;
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live.
       * @param unit The time to live unit.
       * @return The command builder.
       */
      public Builder withTtl(long ttl, TimeUnit unit) {
        command.ttl = unit.toMillis(ttl);
        return this;
      }
    }
  }

  /**
   * Put command.
   */
  public static class Put extends TtlCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, Put> {
      public Builder() {
        super(new Put());
      }
    }
  }

  /**
   * Put if absent command.
   */
  public static class PutIfAbsent extends TtlCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, PutIfAbsent> {
      public Builder() {
        super(new PutIfAbsent());
      }
    }
  }

  /**
   * Get query.
   */
  public static class Get extends KeyQuery<Object> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Get query builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, Get> {
      public Builder() {
        super(new Get());
      }
    }
  }

  /**
   * Get or default query.
   */
  public static class GetOrDefault extends KeyQuery<Object> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private Object defaultValue;

    /**
     * Returns the default value.
     *
     * @return The default value.
     */
    public Object defaultValue() {
      return defaultValue;
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      defaultValue = serializer.readObject(buffer);
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(defaultValue, buffer);
    }

    /**
     * Get command builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, GetOrDefault> {
      public Builder() {
        super(new GetOrDefault());
      }

      /**
       * Sets the default value.
       *
       * @param defaultValue The default value.
       * @return The query builder.
       */
      public Builder withDefaultValue(Object defaultValue) {
        query.defaultValue = defaultValue;
        return this;
      }
    }
  }

  /**
   * Remove command.
   */
  public static class Remove extends KeyValueCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Get command builder.
     */
    public static class Builder extends KeyValueCommand.Builder<Builder, Remove> {
      public Builder() {
        super(new Remove());
      }
    }
  }

  /**
   * Is empty query.
   */
  public static class IsEmpty extends MapQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Is empty command builder.
     */
    public static class Builder extends MapQuery.Builder<Builder, IsEmpty> {
      public Builder() {
        super(new IsEmpty());
      }
    }
  }

  /**
   * Size query.
   */
  public static class Size extends MapQuery<Integer> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Is empty command builder.
     */
    public static class Builder extends MapQuery.Builder<Builder, Size> {
      public Builder() {
        super(new Size());
      }
    }
  }

  /**
   * Clear command.
   */
  public static class Clear extends MapCommand<Void> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Get command builder.
     */
    public static class Builder extends MapCommand.Builder<Builder, Clear> {
      public Builder() {
        super(new Clear());
      }
    }
  }

  /**
   * Map state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.StateMachine {
    private final Map<Object, Commit<? extends TtlCommand>> map = new HashMap<>();
    private final Set<Long> sessions = new HashSet<>();
    private long time;

    /**
     * Updates the wall clock time.
     */
    private void updateTime(Commit<?> commit) {
      time = Math.max(time, commit.timestamp());
    }

    @Override
    public void register(Session session) {
      sessions.add(session.id());
    }

    @Override
    public void expire(Session session) {
      sessions.remove(session.id());
    }

    @Override
    public void close(Session session) {
      sessions.remove(session.id());
    }

    /**
     * Returns a boolean value indicating whether the given commit is active.
     */
    private boolean isActive(Commit<? extends TtlCommand> commit) {
      if (commit == null) {
        return false;
      } else if (commit.operation().mode() == Mode.EPHEMERAL && !sessions.contains(commit.session().id())) {
        return false;
      } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time - commit.timestamp()) {
        return false;
      }
      return true;
    }

    /**
     * Handles a contains key commit.
     */
    @Apply(ContainsKey.class)
    protected boolean containsKey(Commit<ContainsKey> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.get(commit.operation().key());
      if (!isActive(command)) {
        map.remove(commit.operation().key());
        return false;
      }
      return true;
    }

    /**
     * Handles a get commit.
     */
    @Apply(Get.class)
    protected Object get(Commit<Get> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.get(commit.operation().key());
      if (command != null) {
        if (!isActive(command)) {
          map.remove(commit.operation().key());
        } else {
          return command.operation().value();
        }
      }
      return null;
    }

    /**
     * Handles a get or default commit.
     */
    @Apply(GetOrDefault.class)
    protected Object getOrDefault(Commit<GetOrDefault> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.get(commit.operation().key());
      if (command == null) {
        return commit.operation().defaultValue();
      } else if (!isActive(command)) {
        map.remove(commit.operation().key());
      } else {
        return command.operation().value();
      }
      return commit.operation().defaultValue();
    }

    /**
     * Handles a put commit.
     */
    @Apply(Put.class)
    protected Object put(Commit<Put> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.put(commit.operation().key(), commit);
      return isActive(command) ? command.operation().value : null;
    }

    /**
     * Handles a put if absent commit.
     */
    @Apply(PutIfAbsent.class)
    protected Object putIfAbsent(Commit<PutIfAbsent> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.putIfAbsent(commit.operation().key(), commit);
      return isActive(command) ? command.operation().value : null;
    }

    /**
     * Filters a put and put if absent commit.
     */
    @Filter({Put.class, PutIfAbsent.class})
    protected boolean filterPut(Commit<? extends TtlCommand> commit) {
      Commit<? extends TtlCommand> command = map.get(commit.operation().key());
      return isActive(command) && command.index() == commit.index();
    }

    /**
     * Handles a remove commit.
     */
    @Apply(Remove.class)
    protected Object remove(Commit<Remove> commit) {
      updateTime(commit);
      if (commit.operation().value() != null) {
        Commit<? extends TtlCommand> command = map.get(commit.operation().key());
        if (!isActive(command)) {
          map.remove(commit.operation().key());
        } else {
          Object value = command.operation().value();
          if ((value == null && commit.operation().value() == null) || (value != null && commit.operation().value() != null && value.equals(commit.operation().value()))) {
            map.remove(commit.operation().key());
            return true;
          }
          return false;
        }
        return false;
      } else {
        Commit<? extends TtlCommand> command =  map.remove(commit.operation().key());
        return isActive(command) ? command.operation().value() : null;
      }
    }

    /**
     * Filters a remove commit.
     */
    @Filter(value={Remove.class, Clear.class}, compaction=Compaction.Type.MAJOR)
    protected boolean filterRemove(Commit<?> commit, Compaction compaction) {
      return commit.index() > compaction.index();
    }

    /**
     * Handles a size commit.
     */
    @Apply(Size.class)
    protected int size(Commit<Size> commit) {
      updateTime(commit);
      return map.size();
    }

    /**
     * Handles an is empty commit.
     */
    @Apply(IsEmpty.class)
    protected boolean isEmpty(Commit<IsEmpty> commit) {
      updateTime(commit);
      return map.isEmpty();
    }

    /**
     * Handles a clear commit.
     */
    @Apply(Clear.class)
    protected void clear(Commit<Clear> commit) {
      updateTime(commit);
      map.clear();
    }
  }

}
