// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
