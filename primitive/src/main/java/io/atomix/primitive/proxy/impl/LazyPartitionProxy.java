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

import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PartitionProxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Lazy partition proxy.
 */
public class LazyPartitionProxy extends DelegatingPartitionProxy {
  private volatile CompletableFuture<PartitionProxy> connectFuture;

  public LazyPartitionProxy(PartitionProxy proxy) {
    super(proxy);
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return connect().thenCompose(v -> super.execute(operation));
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    connect().thenRun(() -> super.addEventListener(eventType, listener));
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    connect().thenRun(() -> super.removeEventListener(eventType, listener));
  }

  @Override
  public CompletableFuture<PartitionProxy> connect() {
    if (connectFuture == null) {
      synchronized (this) {
        if (connectFuture == null) {
          connectFuture = super.connect();
        }
      }
    }
    return connectFuture;
  }
}
