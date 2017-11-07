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
package io.atomix.primitives.lock.impl;

import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.primitives.lock.AsyncDistributedLock;
import io.atomix.primitives.lock.impl.RaftLockOperations.Lock;
import io.atomix.primitives.lock.impl.RaftLockOperations.Unlock;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.time.Version;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.atomix.primitives.lock.impl.RaftLockOperations.LOCK;
import static io.atomix.primitives.lock.impl.RaftLockOperations.UNLOCK;

/**
 * Raft lock.
 */
public class RaftLock extends AbstractRaftPrimitive implements AsyncDistributedLock {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftLockOperations.NAMESPACE)
      .register(RaftLockEvents.NAMESPACE)
      .build());

  private final Map<Integer, CompletableFuture<Version>> futures = new ConcurrentHashMap<>();
  private final AtomicInteger id = new AtomicInteger();
  private int lock;

  public RaftLock(RaftProxy proxy) {
    super(proxy);
    proxy.addEventListener(RaftLockEvents.LOCK, SERIALIZER::decode, this::handleLocked);
    proxy.addEventListener(RaftLockEvents.FAIL, SERIALIZER::decode, this::handleFailed);
  }

  private void handleLocked(LockEvent event) {
    CompletableFuture<Version> future = futures.remove(event.id());
    if (future != null) {
      this.lock = event.id();
      future.complete(new Version(event.version()));
    }
  }

  private void handleFailed(LockEvent event) {
    CompletableFuture<Version> future = futures.remove(event.id());
    if (future != null) {
      future.complete(null);
    }
  }

  @Override
  public CompletableFuture<Version> lock() {
    CompletableFuture<Version> future = new CompletableFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    proxy.invoke(LOCK, SERIALIZER::encode, new Lock(id, -1)).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    CompletableFuture<Version> future = new CompletableFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    proxy.invoke(LOCK, SERIALIZER::encode, new Lock(id, 0)).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future.thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock(Duration timeout) {
    CompletableFuture<Version> future = new CompletableFuture<>();
    int id = this.id.incrementAndGet();
    futures.put(id, future);
    proxy.invoke(LOCK, SERIALIZER::encode, new Lock(id, timeout.toMillis())).whenComplete((result, error) -> {
      if (error != null) {
        futures.remove(id);
        future.completeExceptionally(error);
      }
    });
    return future.thenApply(Optional::ofNullable);

  }

  @Override
  public CompletableFuture<Void> unlock() {
    int lock = this.lock;
    this.lock = 0;
    if (lock != 0) {
      return proxy.invoke(UNLOCK, SERIALIZER::encode, new Unlock(lock));
    }
    return CompletableFuture.completedFuture(null);
  }
}
