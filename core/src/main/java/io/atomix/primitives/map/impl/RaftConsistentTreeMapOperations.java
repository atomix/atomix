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

package io.atomix.primitives.map.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.time.Versioned;
import io.atomix.utils.Match;

import java.util.AbstractMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link io.atomix.primitives.map.AsyncConsistentTreeMap} Resource
 * state machine operations.
 */
public enum RaftConsistentTreeMapOperations implements OperationId {
  SUB_MAP("subMap", OperationType.QUERY),
  FIRST_KEY("firstKey", OperationType.QUERY),
  LAST_KEY("lastKey", OperationType.QUERY),
  FIRST_ENTRY("firstEntry", OperationType.QUERY),
  LAST_ENTRY("lastEntry", OperationType.QUERY),
  POLL_FIRST_ENTRY("pollFirstEntry", OperationType.QUERY),
  POLL_LAST_ENTRY("pollLastEntry", OperationType.QUERY),
  LOWER_ENTRY("lowerEntry", OperationType.QUERY),
  LOWER_KEY("lowerKey", OperationType.QUERY),
  FLOOR_ENTRY("floorEntry", OperationType.QUERY),
  FLOOR_KEY("floorKey", OperationType.QUERY),
  CEILING_ENTRY("ceilingEntry", OperationType.QUERY),
  CEILING_KEY("ceilingKey", OperationType.QUERY),
  HIGHER_ENTRY("higherEntry", OperationType.QUERY),
  HIGHER_KEY("higherKey", OperationType.QUERY);

  private final String id;
  private final OperationType type;

  RaftConsistentTreeMapOperations(String id, OperationType type) {
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
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
      .register(LowerKey.class)
      .register(LowerEntry.class)
      .register(HigherKey.class)
      .register(HigherEntry.class)
      .register(FloorKey.class)
      .register(FloorEntry.class)
      .register(CeilingKey.class)
      .register(CeilingEntry.class)
      .register(Versioned.class)
      .register(AbstractMap.SimpleImmutableEntry.class)
      .register(Maps.immutableEntry("", "").getClass())
      .build(RaftConsistentTreeMapOperations.class.getSimpleName());

  /**
   * Abstract treeMap command.
   */
  @SuppressWarnings("serial")
  public abstract static class TreeOperation {
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .toString();
    }
  }

  /**
   * Abstract key-based query.
   */
  @SuppressWarnings("serial")
  public abstract static class KeyOperation extends TreeOperation {
    protected String key;

    public KeyOperation(String key) {
      this.key = checkNotNull(key);
    }

    public KeyOperation() {
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
  public abstract static class ValueOperation extends TreeOperation {
    protected byte[] value;

    public ValueOperation() {
    }

    public ValueOperation(byte[] value) {
      this.value = checkNotNull(value);
    }

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
   * Contains key command.
   */
  @SuppressWarnings("serial")
  public static class ContainsKey extends KeyOperation {

    public ContainsKey(String key) {
      super(key);
    }

    public ContainsKey() {
    }
  }

  /**
   * Contains value command.
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
   * AsyncConsistentTreeMap update command.
   */
  @SuppressWarnings("serial")
  public static class UpdateAndGet extends TreeOperation {
    private String key;
    private byte[] value;
    private Match<byte[]> valueMatch;
    private Match<Long> versionMatch;

    public UpdateAndGet() {
    }

    public UpdateAndGet(String key,
                        byte[] value,
                        Match<byte[]> valueMatch,
                        Match<Long> versionMatch) {
      this.key = key;
      this.value = value;
      this.valueMatch = valueMatch;
      this.versionMatch = versionMatch;
    }

    public String key() {
      return this.key;
    }

    public byte[] value() {
      return this.value;
    }

    public Match<byte[]> valueMatch() {
      return this.valueMatch;
    }

    public Match<Long> versionMatch() {
      return this.versionMatch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("key", key)
          .add("value", value)
          .add("valueMatch", valueMatch)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Get query.
   */
  @SuppressWarnings("serial")
  public static class Get extends KeyOperation {
    public Get() {
    }

    public Get(String key) {
      super(key);
    }
  }

  /**
   * Get or default query.
   */
  @SuppressWarnings("serial")
  public static class GetOrDefault extends KeyOperation {
    private byte[] defaultValue;

    public GetOrDefault() {
    }

    public GetOrDefault(String key, byte[] defaultValue) {
      super(key);
      this.defaultValue = defaultValue;
    }

    /**
     * Returns the default value.
     *
     * @return the default value
     */
    public byte[] defaultValue() {
      return defaultValue;
    }
  }

  /**
   * Query returns the entry associated with the largest key less than the
   * passed in key.
   */
  @SuppressWarnings("serial")
  public static class LowerEntry extends KeyOperation {
    public LowerEntry() {
    }

    public LowerEntry(String key) {
      super(key);
    }
  }

  /**
   * Query returns the largest key less than the specified key.
   */
  @SuppressWarnings("serial")
  public static class LowerKey extends KeyOperation {
    public LowerKey() {
    }

    public LowerKey(String key) {
      super(key);
    }
  }

  /**
   * Query returns the entry associated with the largest key smaller than or
   * equal to the specified key.
   */
  @SuppressWarnings("serial")
  public static class FloorEntry extends KeyOperation {
    public FloorEntry() {
    }

    public FloorEntry(String key) {
      super(key);
    }
  }

  /**
   * Query returns the largest key smaller than or equal to the passed in
   * key.
   */
  @SuppressWarnings("serial")
  public static class FloorKey extends KeyOperation {
    public FloorKey() {
    }

    public FloorKey(String key) {
      super(key);
    }
  }

  /**
   * Returns the entry associated with the smallest key larger than or equal
   * to the specified key.
   */
  @SuppressWarnings("serial")
  public static class CeilingEntry extends KeyOperation {
    public CeilingEntry() {
    }

    public CeilingEntry(String key) {
      super(key);
    }
  }

  /**
   * Returns the smallest key larger than or equal to the specified key.
   */
  @SuppressWarnings("serial")
  public static class CeilingKey extends KeyOperation {
    public CeilingKey() {
    }

    public CeilingKey(String key) {
      super(key);
    }
  }

  /**
   * Returns the entry associated with the smallest key larger than the
   * specified key.
   */
  @SuppressWarnings("serial")
  public static class HigherEntry extends KeyOperation {
    public HigherEntry() {
    }

    public HigherEntry(String key) {
      super(key);
    }
  }

  /**
   * Returns the smallest key larger than the specified key.
   */
  @SuppressWarnings("serial")
  public static class HigherKey extends KeyOperation {
    public HigherKey() {
    }

    public HigherKey(String key) {
      super(key);
    }
  }

  @SuppressWarnings("serial")
  public static class SubMap<K, V> extends TreeOperation {
    private K fromKey;
    private K toKey;
    private boolean inclusiveFrom;
    private boolean inclusiveTo;

    public SubMap() {
    }

    public SubMap(K fromKey, K toKey, boolean inclusiveFrom,
                  boolean inclusiveTo) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.inclusiveFrom = inclusiveFrom;
      this.inclusiveTo = inclusiveTo;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("getFromKey", fromKey)
          .add("getToKey", toKey)
          .add("inclusiveFrotBound", inclusiveFrom)
          .add("inclusiveToBound", inclusiveTo)
          .toString();
    }

    public K fromKey() {
      return fromKey;
    }

    public K toKey() {
      return toKey;
    }

    public boolean isInclusiveFrom() {
      return inclusiveFrom;
    }

    public boolean isInclusiveTo() {
      return inclusiveTo;
    }
  }
}
