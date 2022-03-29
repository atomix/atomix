// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.operation;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base type for Raft state operations.
 */
public class PrimitiveOperation {

  /**
   * Creates a new primitive operation with a simplified identifier and a null value.
   *
   * @param id    the operation identifier
   * @return the primitive operation
   */
  public static PrimitiveOperation operation(OperationId id) {
    return new PrimitiveOperation(OperationId.simplify(id), null);
  }

  /**
   * Creates a new primitive operation with a simplified identifier.
   *
   * @param id    the operation identifier
   * @param value the operation value
   * @return the primitive operation
   */
  public static PrimitiveOperation operation(OperationId id, byte[] value) {
    return new PrimitiveOperation(OperationId.simplify(id), value);
  }

  protected final OperationId id;
  protected final byte[] value;

  protected PrimitiveOperation() {
    this.id = null;
    this.value = null;
  }

  public PrimitiveOperation(OperationId id, byte[] value) {
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
    if (object instanceof PrimitiveOperation) {
      PrimitiveOperation operation = (PrimitiveOperation) object;
      return Objects.equals(operation.id, id) && Arrays.equals(operation.value, value);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("value", value != null ? ArraySizeHashPrinter.of(value) : null)
        .toString();
  }
}
