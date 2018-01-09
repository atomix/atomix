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

import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.ArraySizeHashPrinter;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.time.Versioned;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link io.atomix.core.map.ConsistentMap} operations.
 * <p>
 * WARNING: Do not refactor enum values. Only add to them.
 * Changing values risk breaking the ability to backup/restore/upgrade clusters.
 */
public enum ConsistentMapOperations implements OperationId {
  IS_EMPTY(OperationType.QUERY),
  SIZE(OperationType.QUERY),
  CONTAINS_KEY(OperationType.QUERY),
  CONTAINS_VALUE(OperationType.QUERY),
  GET(OperationType.QUERY),
  GET_ALL_PRESENT(OperationType.QUERY),
  GET_OR_DEFAULT(OperationType.QUERY),
  KEY_SET(OperationType.QUERY),
  VALUES(OperationType.QUERY),
  ENTRY_SET(OperationType.QUERY),
  PUT(OperationType.COMMAND),
  PUT_IF_ABSENT(OperationType.COMMAND),
  PUT_AND_GET(OperationType.COMMAND),
  REMOVE(OperationType.COMMAND),
  REMOVE_VALUE(OperationType.COMMAND),
  REMOVE_VERSION(OperationType.COMMAND),
  REPLACE(OperationType.COMMAND),
  REPLACE_VALUE(OperationType.COMMAND),
  REPLACE_VERSION(OperationType.COMMAND),
  CLEAR(OperationType.COMMAND),
  ADD_LISTENER(OperationType.COMMAND),
  REMOVE_LISTENER(OperationType.COMMAND),
  BEGIN(OperationType.COMMAND),
  PREPARE(OperationType.COMMAND),
  PREPARE_AND_COMMIT(OperationType.COMMAND),
  COMMIT(OperationType.COMMAND),
  ROLLBACK(OperationType.COMMAND);

  private final OperationType type;

  ConsistentMapOperations(OperationType type) {
    this.type = type;
  }

  @Override
  public String id() {
    return name();
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(ContainsKey.class)
      .register(ContainsValue.class)
      .register(Get.class)
      .register(GetAllPresent.class)
      .register(GetOrDefault.class)
      .register(Put.class)
      .register(Remove.class)
      .register(RemoveValue.class)
      .register(RemoveVersion.class)
      .register(Replace.class)
      .register(ReplaceValue.class)
      .register(ReplaceVersion.class)
      .register(TransactionBegin.class)
      .register(TransactionPrepare.class)
      .register(TransactionPrepareAndCommit.class)
      .register(TransactionCommit.class)
      .register(TransactionRollback.class)
      .register(TransactionId.class)
      .register(TransactionLog.class)
      .register(MapUpdate.class)
      .register(MapUpdate.Type.class)
      .register(PrepareResult.class)
      .register(CommitResult.class)
      .register(RollbackResult.class)
      .register(MapEntryUpdateResult.class)
      .register(MapEntryUpdateResult.Status.class)
      .register(Versioned.class)
      .register(byte[].class)
      .build(ConsistentMapOperations.class.getSimpleName());

  /**
   * Abstract map command.
   */
  @SuppressWarnings("serial")
  public abstract static class MapOperation {
    @Override
    public String toString() {
      return toStringHelper(getClass())
          .toString();
    }
  }

  /**
   * Abstract key-based query.
   */
  @SuppressWarnings("serial")
  public abstract static class KeyOperation extends MapOperation {
    protected String key;

    public KeyOperation() {
    }

    public KeyOperation(String key) {
      this.key = checkNotNull(key, "key cannot be null");
    }

    /**
     * Returns the key.
     *
     * @return key
     */
    public String key() {
      return key;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("key", key)
          .toString();
    }
  }

  /**
   * Abstract value-based query.
   */
  @SuppressWarnings("serial")
  public abstract static class ValueOperation extends MapOperation {
    protected byte[] value;

    public ValueOperation() {
    }

    public ValueOperation(byte[] value) {
      this.value = value;
    }

    /**
     * Returns the value.
     *
     * @return value
     */
    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("value", value)
          .toString();
    }
  }

  /**
   * Abstract key/value operation.
   */
  @SuppressWarnings("serial")
  public abstract static class KeyValueOperation extends KeyOperation {
    protected byte[] value;

    public KeyValueOperation() {
    }

    public KeyValueOperation(String key, byte[] value) {
      super(key);
      this.value = value;
    }

    /**
     * Returns the value.
     *
     * @return value
     */
    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("key", key)
          .add("value", ArraySizeHashPrinter.of(value))
          .toString();
    }
  }

  /**
   * Abstract key/version operation.
   */
  @SuppressWarnings("serial")
  public abstract static class KeyVersionOperation extends KeyOperation {
    protected long version;

    public KeyVersionOperation() {
    }

    public KeyVersionOperation(String key, long version) {
      super(key);
      this.version = version;
    }

    /**
     * Returns the version.
     *
     * @return version
     */
    public long version() {
      return version;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("key", key)
          .add("version", version)
          .toString();
    }
  }

  /**
   * Contains key command.
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
   * Map put operation.
   */
  public static class Put extends KeyValueOperation {
    private long ttl;

    public Put() {
    }

    public Put(String key, byte[] value, long ttl) {
      super(key, value);
      this.ttl = ttl;
    }

    public long ttl() {
      return ttl;
    }
  }

  /**
   * Remove operation.
   */
  public static class Remove extends KeyOperation {
    public Remove() {
    }

    public Remove(String key) {
      super(key);
    }
  }

  /**
   * Remove if value match operation.
   */
  public static class RemoveValue extends KeyValueOperation {
    public RemoveValue() {
    }

    public RemoveValue(String key, byte[] value) {
      super(key, value);
    }
  }

  /**
   * Remove if version match operation.
   */
  public static class RemoveVersion extends KeyVersionOperation {
    public RemoveVersion() {
    }

    public RemoveVersion(String key, long version) {
      super(key, version);
    }
  }

  /**
   * Replace operation.
   */
  public static class Replace extends KeyValueOperation {
    public Replace() {
    }

    public Replace(String key, byte[] value) {
      super(key, value);
    }
  }

  /**
   * Replace by value operation.
   */
  public static class ReplaceValue extends KeyOperation {
    private byte[] oldValue;
    private byte[] newValue;

    public ReplaceValue() {
    }

    public ReplaceValue(String key, byte[] oldValue, byte[] newValue) {
      super(key);
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    public byte[] oldValue() {
      return oldValue;
    }

    public byte[] newValue() {
      return newValue;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("key", key)
          .add("oldValue", ArraySizeHashPrinter.of(oldValue))
          .add("newValue", ArraySizeHashPrinter.of(newValue))
          .toString();
    }
  }

  /**
   * Replace by version operation.
   */
  public static class ReplaceVersion extends KeyOperation {
    private long oldVersion;
    private byte[] newValue;

    public ReplaceVersion() {
    }

    public ReplaceVersion(String key, long oldVersion, byte[] newValue) {
      super(key);
      this.oldVersion = oldVersion;
      this.newValue = newValue;
    }

    public long oldVersion() {
      return oldVersion;
    }

    public byte[] newValue() {
      return newValue;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("key", key)
          .add("oldVersion", oldVersion)
          .add("newValue", ArraySizeHashPrinter.of(newValue))
          .toString();
    }
  }

  /**
   * Transaction begin command.
   */
  public static class TransactionBegin extends MapOperation {
    private TransactionId transactionId;

    public TransactionBegin() {
    }

    public TransactionBegin(TransactionId transactionId) {
      this.transactionId = transactionId;
    }

    public TransactionId transactionId() {
      return transactionId;
    }
  }

  /**
   * Map prepare command.
   */
  @SuppressWarnings("serial")
  public static class TransactionPrepare extends MapOperation {
    private TransactionLog<MapUpdate<String, byte[]>> transactionLog;

    public TransactionPrepare() {
    }

    public TransactionPrepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
      this.transactionLog = transactionLog;
    }

    public TransactionLog<MapUpdate<String, byte[]>> transactionLog() {
      return transactionLog;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("transactionLog", transactionLog)
          .toString();
    }
  }

  /**
   * Map prepareAndCommit command.
   */
  @SuppressWarnings("serial")
  public static class TransactionPrepareAndCommit extends TransactionPrepare {
    public TransactionPrepareAndCommit() {
    }

    public TransactionPrepareAndCommit(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
      super(transactionLog);
    }
  }

  /**
   * Map transaction commit command.
   */
  @SuppressWarnings("serial")
  public static class TransactionCommit extends MapOperation {
    private TransactionId transactionId;

    public TransactionCommit() {
    }

    public TransactionCommit(TransactionId transactionId) {
      this.transactionId = transactionId;
    }

    /**
     * Returns the transaction identifier.
     *
     * @return transaction id
     */
    public TransactionId transactionId() {
      return transactionId;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("transactionId", transactionId)
          .toString();
    }
  }

  /**
   * Map transaction rollback command.
   */
  @SuppressWarnings("serial")
  public static class TransactionRollback extends MapOperation {
    private TransactionId transactionId;

    public TransactionRollback() {
    }

    public TransactionRollback(TransactionId transactionId) {
      this.transactionId = transactionId;
    }

    /**
     * Returns the transaction identifier.
     *
     * @return transaction id
     */
    public TransactionId transactionId() {
      return transactionId;
    }

    @Override
    public String toString() {
      return toStringHelper(getClass())
          .add("transactionId", transactionId)
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
   * Get all present query.
   */
  @SuppressWarnings("serial")
  public static class GetAllPresent extends MapOperation {
    private Set<String> keys;

    public GetAllPresent() {
    }

    public GetAllPresent(Set<String> keys) {
      this.keys = keys;
    }

    /**
     * Returns the keys.
     *
     * @return the keys
     */
    public Set<String> keys() {
      return keys;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("keys", keys)
          .toString();
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

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("key", key)
          .add("defaultValue", ArraySizeHashPrinter.of(defaultValue))
          .toString();
    }
  }
}
