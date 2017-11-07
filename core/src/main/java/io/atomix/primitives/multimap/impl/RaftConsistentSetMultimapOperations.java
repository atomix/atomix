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

package io.atomix.primitives.multimap.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.time.Versioned;
import io.atomix.utils.Match;

import java.util.ArrayList;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AsyncConsistentMultimap state machine commands.
 */
public enum RaftConsistentSetMultimapOperations implements OperationId {
  GET("get", OperationType.QUERY),
  SIZE("size", OperationType.QUERY),
  IS_EMPTY("isEmpty", OperationType.QUERY),
  CONTAINS_KEY("containsKey", OperationType.QUERY),
  CONTAINS_VALUE("containsValue", OperationType.QUERY),
  CONTAINS_ENTRY("containsEntry", OperationType.QUERY),
  KEY_SET("keySet", OperationType.QUERY),
  KEYS("keys", OperationType.QUERY),
  VALUES("values", OperationType.QUERY),
  ENTRIES("entries", OperationType.QUERY),
  PUT("put", OperationType.COMMAND),
  REMOVE("remove", OperationType.COMMAND),
  REMOVE_ALL("removeAll", OperationType.COMMAND),
  REPLACE("replace", OperationType.COMMAND),
  CLEAR("clear", OperationType.COMMAND),
  ADD_LISTENER("addListener", OperationType.COMMAND),
  REMOVE_LISTENER("removeListener", OperationType.COMMAND);

  private final String id;
  private final OperationType type;

  RaftConsistentSetMultimapOperations(String id, OperationType type) {
    this.id = id;
    this.type = type;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(ContainsEntry.class)
      .register(ContainsKey.class)
      .register(ContainsValue.class)
      .register(Get.class)
      .register(MultiRemove.class)
      .register(Put.class)
      .register(RemoveAll.class)
      .register(Replace.class)
      .register(Match.class)
      .register(Versioned.class)
      .register(ArrayList.class)
      .register(Maps.immutableEntry("", "").getClass())
      .build(RaftConsistentSetMultimap.class.getSimpleName());

  /**
   * Abstract multimap command.
   */
  @SuppressWarnings("serial")
  public abstract static class MultimapOperation {
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .toString();
    }
  }

  /**
   * Abstract key-based multimap query.
   */
  @SuppressWarnings("serial")
  public abstract static class KeyOperation extends MultimapOperation {
    protected String key;

    public KeyOperation() {
    }

    public KeyOperation(String key) {
      this.key = checkNotNull(key);
    }

    public String key() {
      return key;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .toString();
    }
  }

  /**
   * Abstract value-based query.
   */
  @SuppressWarnings("serial")
  public abstract static class ValueOperation extends MultimapOperation {
    protected byte[] value;

    public ValueOperation() {
    }

    public ValueOperation(byte[] value) {
      this.value = checkNotNull(value);
    }

    /**
     * Returns the value.
     *
     * @return value.
     */
    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("value", value)
          .toString();
    }
  }

  /**
   * Contains key query.
   */
  @SuppressWarnings("serial")
  public static class ContainsKey extends KeyOperation {
    public ContainsKey() {
    }

    public ContainsKey(String key) {
      super(key);
    }
  }

  /**
   * Contains value query.
   */
  @SuppressWarnings("serial")
  public static class ContainsValue extends ValueOperation {
    public ContainsValue() {
    }

    public ContainsValue(byte[] value) {
      super(value);
    }
  }

  /**
   * Contains entry query.
   */
  @SuppressWarnings("serial")
  public static class ContainsEntry extends MultimapOperation {
    protected String key;
    protected byte[] value;

    public ContainsEntry() {
    }

    public ContainsEntry(String key, byte[] value) {
      this.key = checkNotNull(key);
      this.value = checkNotNull(value);
    }

    public String key() {
      return key;
    }

    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("value", value)
          .toString();
    }
  }

  /**
   * Remove command, backs remove and removeAll's that return booleans.
   */
  @SuppressWarnings("serial")
  public static class RemoveAll extends MultimapOperation {
    private String key;
    private Match<Long> versionMatch;

    public RemoveAll() {
    }

    public RemoveAll(String key, Match<Long> versionMatch) {
      this.key = checkNotNull(key);
      this.versionMatch = versionMatch;
    }

    public String key() {
      return this.key;
    }

    public Match<Long> versionMatch() {
      return versionMatch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Remove command, backs remove and removeAll's that return booleans.
   */
  @SuppressWarnings("serial")
  public static class MultiRemove extends MultimapOperation {
    private String key;
    private Collection<byte[]> values;
    private Match<Long> versionMatch;

    public MultiRemove() {
    }

    public MultiRemove(String key, Collection<byte[]> valueMatches,
                       Match<Long> versionMatch) {
      this.key = checkNotNull(key);
      this.values = valueMatches;
      this.versionMatch = versionMatch;
    }

    public String key() {
      return this.key;
    }

    public Collection<byte[]> values() {
      return values;
    }

    public Match<Long> versionMatch() {
      return versionMatch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("values", values)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Command to back the put and putAll methods.
   */
  @SuppressWarnings("serial")
  public static class Put extends MultimapOperation {
    private String key;
    private Collection<? extends byte[]> values;
    private Match<Long> versionMatch;

    public Put() {
    }

    public Put(String key, Collection<? extends byte[]> values, Match<Long> versionMatch) {
      this.key = checkNotNull(key);
      this.values = values;
      this.versionMatch = versionMatch;
    }

    public String key() {
      return key;
    }

    public Collection<? extends byte[]> values() {
      return values;
    }

    public Match<Long> versionMatch() {
      return versionMatch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("values", values)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Replace command, returns the collection that was replaced.
   */
  @SuppressWarnings("serial")
  public static class Replace extends MultimapOperation {
    private String key;
    private Collection<byte[]> values;
    private Match<Long> versionMatch;

    public Replace() {
    }

    public Replace(String key, Collection<byte[]> values,
                   Match<Long> versionMatch) {
      this.key = checkNotNull(key);
      this.values = values;
      this.versionMatch = versionMatch;
    }

    public String key() {
      return this.key;
    }

    public Match<Long> versionMatch() {
      return versionMatch;
    }

    public Collection<byte[]> values() {
      return values;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("values", values)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Get value query.
   */
  public static class Get extends KeyOperation {
    public Get() {
    }

    public Get(String key) {
      super(key);
    }
  }
}
