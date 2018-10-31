/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.proxy.impl;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.session.SessionId;

/**
 * Log operation.
 */
public class LogOperation {
  private final SessionId sessionId;
  private final long operationIndex;
  private final OperationId operationId;
  private final byte[] operation;

  public LogOperation(SessionId sessionId, long operationIndex, OperationId operationId, byte[] operation) {
    this.sessionId = sessionId;
    this.operationIndex = operationIndex;
    this.operationId = operationId;
    this.operation = operation;
  }

  public SessionId sessionId() {
    return sessionId;
  }

  public long operationIndex() {
    return operationIndex;
  }

  public OperationId operationId() {
    return operationId;
  }

  public byte[] operation() {
    return operation;
  }
}
