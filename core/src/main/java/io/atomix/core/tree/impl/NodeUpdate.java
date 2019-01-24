/*
 * Copyright 2017-present Open Networking Foundation

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

package io.atomix.core.tree.impl;

import com.google.common.base.MoreObjects;
import io.atomix.core.tree.DocumentPath;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Map update operation.
 *
 * @param <V> map value type
 */
public final class NodeUpdate<V> {

  /**
   * Type of database update operation.
   */
  public enum Type {
    /**
     * Creates an entry if the current version matches specified version.
     */
    CREATE_NODE,
    /**
     * Updates an entry if the current version matches specified version.
     */
    UPDATE_NODE,
    /**
     * Deletes an entry if the current version matches specified version.
     */
    DELETE_NODE
  }

  private Type type;
  private DocumentPath path;
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
   * Returns the item path being updated.
   *
   * @return item path
   */
  public DocumentPath path() {
    return path;
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
   * Transforms this instance into an instance of different parameterized types.
   *
   * @param valueMapper transcoder to value type
   * @param <T>         value type of returned instance
   * @return new instance
   */
  public <T> NodeUpdate<T> map(Function<V, T> valueMapper) {
    return NodeUpdate.<T>builder()
        .withType(type)
        //.withKey(keyMapper.apply(key))
        .withValue(value == null ? null : valueMapper.apply(value))
        .withVersion(version)
        .build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, path, value, version);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NodeUpdate) {
      NodeUpdate that = (NodeUpdate) object;
      return this.type == that.type
          && Objects.equals(this.path, that.path)
          && Objects.equals(this.value, that.value)
          && Objects.equals(this.version, that.version);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("path", path)
        .add("value", value instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) value) : value)
        .add("version", version)
        .toString();
  }

  /**
   * Creates a new builder instance.
   *
   * @param <V> value type
   * @return builder.
   */
  public static <V> Builder<V> builder() {
    return new Builder<>();
  }

  /**
   * NodeUpdate builder.
   *
   * @param <V> value type
   */
  public static final class Builder<V> {

    private NodeUpdate<V> update = new NodeUpdate<>();

    public NodeUpdate<V> build() {
      validateInputs();
      return update;
    }

    public Builder<V> withType(Type type) {
      update.type = checkNotNull(type, "type cannot be null");
      return this;
    }

    public Builder<V> withPath(DocumentPath key) {
      update.path = checkNotNull(key, "key cannot be null");
      return this;
    }

    public Builder<V> withValue(V value) {
      update.value = value;
      return this;
    }

    public Builder<V> withVersion(long version) {
      update.version = version;
      return this;
    }

    private void validateInputs() {
      checkNotNull(update.type, "type must be specified");
      switch (update.type) {
        case CREATE_NODE:
          checkNotNull(update.path, "key must be specified");
          checkNotNull(update.value, "value must be specified.");
          break;
        case UPDATE_NODE:
          checkNotNull(update.path, "key must be specified");
          checkNotNull(update.value, "value must be specified.");
          checkState(update.version >= 0, "version must be specified");
          break;
        case DELETE_NODE:
          checkNotNull(update.path, "key must be specified");
          checkState(update.version >= 0, "version must be specified");
          break;
        default:
          throw new IllegalStateException("Unknown operation type");
      }

    }
  }
}
