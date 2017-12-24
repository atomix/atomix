/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.atomix.core.map.impl;

import com.google.common.base.MoreObjects;
import io.atomix.utils.ArraySizeHashPrinter;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Map update operation.
 *
 * @param <K> map key type
 * @param <V> map value type
 */
public final class MapUpdate<K, V> {

  /**
   * Type of database update operation.
   */
  public enum Type {

    /**
     * Acquires a read lock on a key.
     * <p>
     * This record type will check to ensure that the lock version matches the current version for the key
     * in the map and acquire a read lock on the key for the duration of the transaction.
     */
    LOCK,

    /**
     * Checks the version of a key without locking the key.
     * <p>
     * This record type will perform a simple version check during the prepare phase of the two-phase commit
     * protocol to ensure that the key has not changed during a transaction.
     */
    VERSION_MATCH,

    /**
     * Updates an entry if the current version matches specified version.
     */
    PUT_IF_VERSION_MATCH,

    /**
     * Removes an entry if the current version matches specified version.
     */
    REMOVE_IF_VERSION_MATCH,

    /**
     * Removes an entry if the current version matches specified version.
     */
    PUT_IF_ABSENT,
  }

  private Type type;
  private K key;
  private V value;
  private long version = -1;

  /**
   * Returns the type of update operation.
   *
   * @return type of update.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the item key being updated.
   *
   * @return item key
   */
  public K key() {
    return key;
  }

  /**
   * Returns the new value.
   *
   * @return item's target value.
   */
  public V value() {
    return value;
  }

  /**
   * Returns the expected current version in the database for the key.
   *
   * @return expected version.
   */
  public long version() {
    return version;
  }

  /**
   * Transforms this instance into an instance of different paramterized types.
   *
   * @param keyMapper   transcoder for key type
   * @param valueMapper transcoder to value type
   * @param <S>         key type of returned instance
   * @param <T>         value type of returned instance
   * @return new instance
   */
  public <S, T> MapUpdate<S, T> map(Function<K, S> keyMapper, Function<V, T> valueMapper) {
    return MapUpdate.<S, T>builder()
        .withType(type)
        .withKey(keyMapper.apply(key))
        .withValue(value == null ? null : valueMapper.apply(value))
        .withVersion(version)
        .build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, key, value, version);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof MapUpdate) {
      MapUpdate that = (MapUpdate) object;
      return this.type == that.type
          && Objects.equals(this.key, that.key)
          && Objects.equals(this.value, that.value)
          && Objects.equals(this.version, that.version);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("key", key)
        .add("value", value instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) value) : value)
        .add("version", version)
        .toString();
  }

  /**
   * Creates a new builder instance.
   *
   * @param <K> key type
   * @param <V> value type
   * @return builder.
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /**
   * MapUpdate builder.
   *
   * @param <K> key type
   * @param <V> value type
   */
  public static final class Builder<K, V> {

    private MapUpdate<K, V> update = new MapUpdate<>();

    public MapUpdate<K, V> build() {
      validateInputs();
      return update;
    }

    public Builder<K, V> withType(Type type) {
      update.type = checkNotNull(type, "type cannot be null");
      return this;
    }

    public Builder<K, V> withKey(K key) {
      update.key = checkNotNull(key, "key cannot be null");
      return this;
    }

    public Builder<K, V> withValue(V value) {
      update.value = value;
      return this;
    }

    public Builder<K, V> withVersion(long version) {
      update.version = version;
      return this;
    }

    private void validateInputs() {
      checkNotNull(update.type, "type must be specified");
      switch (update.type) {
        case VERSION_MATCH:
          break;
        case LOCK:
          checkNotNull(update.key, "key must be specified");
          checkState(update.version >= 0, "version must be specified");
          break;
        case PUT_IF_ABSENT:
          checkNotNull(update.key, "key must be specified");
          checkNotNull(update.value, "value must be specified.");
          break;
        case PUT_IF_VERSION_MATCH:
          checkNotNull(update.key, "key must be specified");
          checkNotNull(update.value, "value must be specified.");
          checkState(update.version >= 0, "version must be specified");
          break;
        case REMOVE_IF_VERSION_MATCH:
          checkNotNull(update.key, "key must be specified");
          checkState(update.version >= 0, "version must be specified");
          break;
        default:
          throw new IllegalStateException("Unknown operation type");
      }

    }
  }
}
