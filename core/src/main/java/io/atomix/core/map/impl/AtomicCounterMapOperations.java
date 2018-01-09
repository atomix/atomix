/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

/**
 * {@link io.atomix.core.map.AtomicCounterMap} operations.
 * <p>
 * WARNING: Do not refactor enum values. Only add to them.
 * Changing values risk breaking the ability to backup/restore/upgrade clusters.
 */
public enum AtomicCounterMapOperations implements OperationId {
  PUT(OperationType.COMMAND),
  PUT_IF_ABSENT(OperationType.COMMAND),
  GET(OperationType.QUERY),
  REPLACE(OperationType.COMMAND),
  REMOVE(OperationType.COMMAND),
  REMOVE_VALUE(OperationType.COMMAND),
  GET_AND_INCREMENT(OperationType.COMMAND),
  GET_AND_DECREMENT(OperationType.COMMAND),
  INCREMENT_AND_GET(OperationType.COMMAND),
  DECREMENT_AND_GET(OperationType.COMMAND),
  ADD_AND_GET(OperationType.COMMAND),
  GET_AND_ADD(OperationType.COMMAND),
  SIZE(OperationType.QUERY),
  IS_EMPTY(OperationType.QUERY),
  CLEAR(OperationType.COMMAND);

  private final OperationType type;

  AtomicCounterMapOperations(OperationType type) {
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
      .register(IncrementAndGet.class)
      .register(DecrementAndGet.class)
      .register(GetAndIncrement.class)
      .register(GetAndDecrement.class)
      .register(AddAndGet.class)
      .register(GetAndAdd.class)
      .register(Get.class)
      .register(Put.class)
      .register(PutIfAbsent.class)
      .register(Replace.class)
      .register(Remove.class)
      .register(RemoveValue.class)
      .build(AtomicCounterMapOperations.class.getSimpleName());

  public abstract static class AtomicCounterMapOperation<V> {
  }

  public abstract static class KeyOperation extends AtomicCounterMapOperation {
    private String key;

    public KeyOperation() {
    }

    public KeyOperation(String key) {
      this.key = key;
    }

    public String key() {
      return key;
    }
  }

  public static class KeyValueOperation extends KeyOperation {
    private long value;

    public KeyValueOperation() {
    }

    public KeyValueOperation(String key, long value) {
      super(key);
      this.value = value;
    }

    public long value() {
      return value;
    }
  }

  public static class Get extends KeyOperation {
    public Get() {
    }

    public Get(String key) {
      super(key);
    }
  }

  public static class Put extends KeyValueOperation {
    public Put() {
    }

    public Put(String key, long value) {
      super(key, value);
    }
  }

  public static class PutIfAbsent extends KeyValueOperation {
    public PutIfAbsent() {
    }

    public PutIfAbsent(String key, long value) {
      super(key, value);
    }
  }

  public static class Replace extends KeyOperation {
    private long replace;
    private long value;

    public Replace() {
    }

    public Replace(String key, long replace, long value) {
      super(key);
      this.replace = replace;
      this.value = value;
    }

    public long replace() {
      return replace;
    }

    public long value() {
      return value;
    }
  }

  public static class Remove extends KeyOperation {
    public Remove() {
    }

    public Remove(String key) {
      super(key);
    }
  }

  public static class RemoveValue extends KeyValueOperation {
    public RemoveValue() {
    }

    public RemoveValue(String key, long value) {
      super(key, value);
    }
  }

  public static class IncrementAndGet extends KeyOperation {
    public IncrementAndGet() {
    }

    public IncrementAndGet(String key) {
      super(key);
    }
  }

  public static class DecrementAndGet extends KeyOperation {
    public DecrementAndGet(String key) {
      super(key);
    }

    public DecrementAndGet() {
    }
  }

  public static class GetAndIncrement extends KeyOperation {
    public GetAndIncrement() {
    }

    public GetAndIncrement(String key) {
      super(key);
    }
  }

  public static class GetAndDecrement extends KeyOperation {
    public GetAndDecrement() {
    }

    public GetAndDecrement(String key) {
      super(key);
    }
  }

  public abstract static class DeltaOperation extends KeyOperation {
    private long delta;

    public DeltaOperation() {
    }

    public DeltaOperation(String key, long delta) {
      super(key);
      this.delta = delta;
    }

    public long delta() {
      return delta;
    }
  }

  public static class AddAndGet extends DeltaOperation {
    public AddAndGet() {
    }

    public AddAndGet(String key, long delta) {
      super(key, delta);
    }
  }

  public static class GetAndAdd extends DeltaOperation {
    public GetAndAdd() {
    }

    public GetAndAdd(String key, long delta) {
      super(key, delta);
    }
  }
}