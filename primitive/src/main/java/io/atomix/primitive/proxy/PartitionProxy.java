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
package io.atomix.primitive.proxy;

import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionId;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Partition proxy.
 */
public interface PartitionProxy extends Proxy<PartitionProxy> {

  /**
   * Returns the proxy session identifier.
   *
   * @return The proxy session identifier
   */
  SessionId sessionId();

  /**
   * Returns the partition identifier.
   *
   * @return the partition identifier.
   */
  PartitionId partitionId();

  /**
   * Executes an operation to the cluster.
   *
   * @param operation the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  CompletableFuture<byte[]> execute(PrimitiveOperation operation);

  /**
   * Adds an event listener.
   *
   * @param eventType the event type for which to add the listener
   * @param listener  the event listener to add
   */
  void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener);

  /**
   * Removes an event listener.
   *
   * @param eventType the event type for which to remove the listener
   * @param listener  the event listener to remove
   */
  void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener);

  /**
   * Partition proxy builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<PartitionProxy> {
  }
}
