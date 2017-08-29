/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.operation;

import io.atomix.utils.ArraySizeHashPrinter;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base type for Raft state operations.
 */
public class RaftOperation {
  protected final OperationId id;
  protected final byte[] value;

  protected RaftOperation() {
    this.id = null;
    this.value = null;
  }

  public RaftOperation(OperationId id, byte[] value) {
    this.id = id;
    this.value = value;
  }

  /**
   * Returns the operation identifier.
   *
   * @return the operation identifier
   */
  public OperationId id() {
    return id;
  }

  /**
   * Returns the operation value.
   *
   * @return the operation value
   */
  public byte[] value() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), id, value);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RaftOperation) {
      RaftOperation operation = (RaftOperation) object;
      return Objects.equals(operation.id, id) && Objects.equals(operation.value, value);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
