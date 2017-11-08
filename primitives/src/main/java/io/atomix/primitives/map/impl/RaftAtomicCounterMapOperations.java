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
package io.atomix.primitives.map.impl;

import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

/**
 * Atomic counter map commands.
 */
public enum RaftAtomicCounterMapOperations implements OperationId {
  PUT("put", OperationType.COMMAND),
  PUT_IF_ABSENT("putIfAbsent", OperationType.COMMAND),
  GET("get", OperationType.QUERY),
  REPLACE("replace", OperationType.COMMAND),
  REMOVE("remove", OperationType.COMMAND),
  REMOVE_VALUE("removeValue", OperationType.COMMAND),
  GET_AND_INCREMENT("getAndIncrement", OperationType.COMMAND),
  GET_AND_DECREMENT("getAndDecrement", OperationType.COMMAND),
  INCREMENT_AND_GET("incrementAndGet", OperationType.COMMAND),
  DECREMENT_AND_GET("decrementAndGet", OperationType.COMMAND),
  ADD_AND_GET("addAndGet", OperationType.COMMAND),
  GET_AND_ADD("getAndAdd", OperationType.COMMAND),
  SIZE("size", OperationType.QUERY),
  IS_EMPTY("isEmpty", OperationType.QUERY),
  CLEAR("clear", OperationType.COMMAND);

  private final String id;
  private final OperationType type;

  RaftAtomicCounterMapOperations(String id, OperationType type) {
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
      .build(RaftAtomicCounterMapOperations.class.getSimpleName());

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