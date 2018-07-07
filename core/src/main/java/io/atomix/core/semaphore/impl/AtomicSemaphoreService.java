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
package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.operation.Operation;
import io.atomix.primitive.operation.OperationType;

import java.util.Map;

/**
 * Distributed semaphore service.
 */
public interface AtomicSemaphoreService {

  @Operation(value = "acquire", type = OperationType.COMMAND)
  void acquire(long id, int permits, long timeout);

  @Operation(value = "release", type = OperationType.COMMAND)
  void release(int permits);

  @Operation(value = "drain", type = OperationType.COMMAND)
  int drain();

  @Operation(value = "increase", type = OperationType.COMMAND)
  int increase(int permits);

  @Operation(value = "reduce", type = OperationType.COMMAND)
  int reduce(int permits);

  @Operation(value = "available", type = OperationType.QUERY)
  int available();

  @Operation(value = "queueStatus", type = OperationType.QUERY)
  QueueStatus queueStatus();

  @Operation(value = "holderStatus", type = OperationType.QUERY)
  Map<Long, Integer> holderStatus();

}
