/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.PrimitiveOperation;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Execute operation.
 */
public class ExecuteOperation extends BackupOperation {
  private final long session;
  private final MemberId node;
  private final PrimitiveOperation operation;

  public ExecuteOperation(long index, long timestamp, long session, MemberId node, PrimitiveOperation operation) {
    super(Type.EXECUTE, index, timestamp);
    this.session = session;
    this.node = node;
    this.operation = operation;
  }

  public long session() {
    return session;
  }

  public MemberId node() {
    return node;
  }

  public PrimitiveOperation operation() {
    return operation;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .add("session", session)
        .add("node", node)
        .add("operation", operation)
        .toString();
  }
}
