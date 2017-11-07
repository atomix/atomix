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
package io.atomix.primitives.counter.impl;

import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

/**
 * Counter commands.
 */
public enum RaftCounterOperations implements OperationId {
  SET("set", OperationType.COMMAND),
  COMPARE_AND_SET("compareAndSet", OperationType.COMMAND),
  INCREMENT_AND_GET("incrementAndGet", OperationType.COMMAND),
  GET_AND_INCREMENT("getAndIncrement", OperationType.COMMAND),
  ADD_AND_GET("addAndGet", OperationType.COMMAND),
  GET_AND_ADD("getAndAdd", OperationType.COMMAND),
  GET("get", OperationType.QUERY);

  private final String id;
  private final OperationType type;

  RaftCounterOperations(String id, OperationType type) {
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
      .register(Get.class)
      .register(Set.class)
      .register(CompareAndSet.class)
      .register(AddAndGet.class)
      .register(GetAndAdd.class)
      .build(RaftCounterOperations.class.getSimpleName());

  /**
   * Abstract value command.
   */
  public abstract static class ValueOperation {
  }

  /**
   * Get query.
   */
  public static class Get extends ValueOperation {
  }

  /**
   * Set command.
   */
  public static class Set extends ValueOperation {
    private Long value;

    public Set() {
    }

    public Set(Long value) {
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public Long value() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }
  }

  /**
   * Compare and set command.
   */
  public static class CompareAndSet extends ValueOperation {
    private Long expect;
    private Long update;

    public CompareAndSet() {
    }

    public CompareAndSet(Long expect, Long update) {
      this.expect = expect;
      this.update = update;
    }

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public Long expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public Long update() {
      return update;
    }

    @Override
    public String toString() {
      return String.format("%s[expect=%s, update=%s]", getClass().getSimpleName(), expect, update);
    }
  }

  /**
   * Delta command.
   */
  public abstract static class DeltaOperation extends ValueOperation {
    private long delta;

    public DeltaOperation() {
    }

    public DeltaOperation(long delta) {
      this.delta = delta;
    }

    /**
     * Returns the delta.
     *
     * @return The delta.
     */
    public long delta() {
      return delta;
    }
  }

  /**
   * Get and add command.
   */
  public static class GetAndAdd extends DeltaOperation {
    public GetAndAdd() {
    }

    public GetAndAdd(long delta) {
      super(delta);
    }
  }

  /**
   * Add and get command.
   */
  public static class AddAndGet extends DeltaOperation {
    public AddAndGet() {
    }

    public AddAndGet(long delta) {
      super(delta);
    }
  }
}