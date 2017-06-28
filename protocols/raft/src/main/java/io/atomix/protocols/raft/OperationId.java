/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft;

import io.atomix.utils.Identifier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft operation identifier.
 */
public class OperationId extends Identifier<String> {
  public static final OperationId NOOP = new OperationId(OperationType.COMMAND);

  /**
   * Returns a new command operation identifier.
   *
   * @param id the command identifier
   * @return the operation identifier
   */
  public static OperationId command(String id) {
    return from(id, OperationType.COMMAND);
  }

  /**
   * Returns a new query operation identifier.
   *
   * @param id the query identifier
   * @return the operation identifier
   */
  public static OperationId query(String id) {
    return from(id, OperationType.QUERY);
  }

  /**
   * Returns a new operation identifier.
   *
   * @param id the operation name
   * @param type the operation type
   * @return the operation identifier
   */
  public static OperationId from(String id, OperationType type) {
    return new OperationId(id, type);
  }

  private final OperationType type;

  protected OperationId() {
    this.type = null;
  }

  private OperationId(OperationType type) {
    this.type = type;
  }

  protected OperationId(String id, OperationType type) {
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
