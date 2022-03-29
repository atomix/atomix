// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.operation.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.AbstractIdentifier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft operation identifier.
 */
public class DefaultOperationId extends AbstractIdentifier<String> implements OperationId {
  private final OperationType type;

  protected DefaultOperationId() {
    this.type = null;
  }

  public DefaultOperationId(String id, OperationType type) {
    super(id);
    this.type = type;
  }

  /**
   * Returns the operation type.
   *
   * @return the operation type
   */
  public OperationType type() {
    return type;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .add("type", type())
        .toString();
  }
}
