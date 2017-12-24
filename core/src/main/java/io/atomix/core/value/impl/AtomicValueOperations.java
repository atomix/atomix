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
package io.atomix.core.value.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.ArraySizeHashPrinter;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Counter commands.
 */
public enum AtomicValueOperations implements OperationId {
  GET("get", OperationType.QUERY),
  SET("set", OperationType.COMMAND),
  COMPARE_AND_SET("compareAndSet", OperationType.COMMAND),
  GET_AND_SET("getAndSet", OperationType.COMMAND),
  ADD_LISTENER("addListener", OperationType.COMMAND),
  REMOVE_LISTENER("removeListener", OperationType.COMMAND);

  private final String id;
  private final OperationType type;

  AtomicValueOperations(String id, OperationType type) {
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

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(Get.class)
      .register(Set.class)
      .register(CompareAndSet.class)
      .register(GetAndSet.class)
      .build(AtomicValueOperations.class.getSimpleName());

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
    private byte[] value;

    public Set() {
    }

    public Set(byte[] value) {
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("value", ArraySizeHashPrinter.of(value))
          .toString();
    }
  }

  /**
   * Compare and set command.
   */
  public static class CompareAndSet extends ValueOperation {
    private byte[] expect;
    private byte[] update;

    public CompareAndSet() {
    }

    public CompareAndSet(byte[] expect, byte[] update) {
      this.expect = expect;
      this.update = update;
    }

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public byte[] expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public byte[] update() {
      return update;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("expect", ArraySizeHashPrinter.of(expect))
          .add("update", ArraySizeHashPrinter.of(update))
          .toString();
    }
  }

  /**
   * Get and set operation.
   */
  public static class GetAndSet extends ValueOperation {
    private byte[] value;

    public GetAndSet() {
    }

    public GetAndSet(byte[] value) {
      this.value = value;
    }

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public byte[] value() {
      return value;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("value", ArraySizeHashPrinter.of(value))
          .toString();
    }
  }
}