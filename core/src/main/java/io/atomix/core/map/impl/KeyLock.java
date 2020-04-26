/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Scheduled;

/**
 * Map key lock.
 */
class KeyLock<K> {
  private final PartitionId partitionId;
  private final K key;
  private final Supplier<Integer> lockIdGenerator;
  private final ProxyClient<? extends AtomicMapService<K>> client;
  private final AtomicInteger lock = new AtomicInteger();
  private final Map<Integer, LockFuture> futures = Maps.newConcurrentMap();

  KeyLock(PartitionId partitionId, K key, Supplier<Integer> lockIdGenerator, ProxyClient<? extends AtomicMapService<K>> client) {
    this.partitionId = partitionId;
    this.key = key;
    this.lockIdGenerator = lockIdGenerator;
    this.client = client;
  }

  /**
   * Returns a boolean indicating whether the lock is unused.
   *
   * @return indicates whether the lock is unused
   */
  boolean isObsolete() {
    return lock.get() == 0 && futures.isEmpty();
  }

  /**
   * Handles a primitive state change.
   *
   * @param state the primitive state change
   */
  void change(PrimitiveState state) {
    if (state != PrimitiveState.CONNECTED) {
      for (LockFuture future : futures.values()) {
        client.acceptOn(partitionId, service -> service.unlock(key, future.id()));
        future.completeExceptionally(new PrimitiveException.Unavailable());
      }
    }
  }

  /**
   * Handles a lock event.
   *
   * @param id the lock ID
   * @param version the lock version
   */
  void locked(int id, long version) {
    // Remove the LockAttempt from the attempts map and complete it with the lock version if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockFuture attempt = futures.remove(id);
    if (attempt != null) {
      attempt.complete(version);
    } else {
      client.acceptOn(partitionId, service -> service.unlock(key, id));
    }
  }

  /**
   * Handles a lock failed event.
   *
   * @param id the lock ID
   */
  void failed(int id) {
    // Remove the LockAttempt from the attempts map and complete it with a null value if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockFuture attempt = futures.remove(id);
    if (attempt != null) {
      attempt.complete(null);
    }
  }

  /**
   * Locks the key.
   *
   * @return a future to be completed once the key has been locked
   */
  CompletableFuture<Long> lock() {
    // Create and register a new attempt and invoke the LOCK operation on the replicated state machine.
    LockFuture future = new LockFuture();
    client.acceptOn(partitionId, service -> service.lock(key, future.id(), -1)).whenComplete((result, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Attempts to lock the key.
   *
   * @return a future to be completed once the lock attempt is complete
   */
  CompletableFuture<OptionalLong> tryLock() {
    // If the proxy is currently disconnected from the cluster, we can just fail the lock attempt here.
    PrimitiveState state = client.getPartition(partitionId).getState();
    if (state != PrimitiveState.CONNECTED) {
      return CompletableFuture.completedFuture(OptionalLong.empty());
    }

    // Create and register a new attempt and invoke the LOCK operation on teh replicated state machine with
    // a 0 timeout. The timeout will cause the state machine to immediately reject the request if the lock is
    // already owned by another process.
    LockFuture future = new LockFuture();
    client.acceptOn(partitionId, service -> service.lock(key, future.id(), 0)).whenComplete((result, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      }
    });
    return future.thenApply(v -> v != null ? OptionalLong.of(v) : OptionalLong.empty());
  }

  /**
   * Attempts to lock the key with a timeout.
   *
   * @param timeout the lock timeout
   * @return a future to be completed once the lock attempt is complete
   */
  CompletableFuture<OptionalLong> tryLock(Duration timeout) {
    // Create a lock attempt with a client-side timeout and fail the lock if the timer expires.
    // Because time does not progress at the same rate on different nodes, we can't guarantee that
    // the lock won't be granted to this process after it's expired here. Thus, if this timer expires and
    // we fail the lock on the client, we also still need to send an UNLOCK command to the cluster in case it's
    // later granted by the cluster. Note that the semantics of the Raft client will guarantee this operation
    // occurs after any prior LOCK attempt, and the Raft client will retry the UNLOCK request until successful.
    // Additionally, sending the unique lock ID with the command ensures we won't accidentally unlock a different
    // lock call also granted to this process.
    LockFuture future = new LockFuture(timeout, a -> {
      a.complete(null);
      client.acceptOn(partitionId, service -> service.unlock(key, a.id()));
    });

    // Invoke the LOCK operation on the replicated state machine with the given timeout. If the lock is currently
    // held by another process, the state machine will add the attempt to a queue and publish a FAILED event if
    // the timer expires before this process can be granted the lock. If the client cannot reach the Raft cluster,
    // the client-side timer will expire the attempt.
    client.acceptOn(partitionId, service -> service.lock(key, future.id(), timeout.toMillis()))
        .whenComplete((result, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          }
        });
    return future.thenApply(v -> v != null ? OptionalLong.of(v) : OptionalLong.empty());
  }

  /**
   * Unlocks the key.
   *
   * @return a future to be completed once the key has been unlocked
   */
  CompletableFuture<Void> unlock() {
    // Use the current lock ID to ensure we only unlock the lock currently held by this process.
    int lock = this.lock.getAndSet(0);
    if (lock != 0) {
      return client.acceptOn(partitionId, service -> service.unlock(key, lock));
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Checks whether the key is currently locked.
   *
   * @return a future to be completed with a boolean indicating whether the key is locked
   */
  CompletableFuture<Boolean> isLocked() {
    return isLocked(0);
  }

  /**
   * Checks whether the key is currently locked at the given version.
   *
   * @param version the version to check
   * @return a future to be completed with a boolean indicating whether the key is locked
   */
  CompletableFuture<Boolean> isLocked(long version) {
    return client.applyOn(partitionId, service -> service.isLocked(key, version));
  }

  /**
   * Lock future.
   */
  private class LockFuture extends CompletableFuture<Long> {
    private final int id;
    private final Scheduled scheduled;

    LockFuture() {
      this(null, null);
    }

    LockFuture(Duration duration, Consumer<LockFuture> callback) {
      this.id = lockIdGenerator.get();
      this.scheduled = duration != null && callback != null
          ? client.getPartition(partitionId).context().schedule(duration, () -> callback.accept(this)) : null;
      futures.put(id, this);
    }

    /**
     * Returns the lock attempt ID.
     *
     * @return the lock attempt ID
     */
    int id() {
      return id;
    }

    @Override
    public boolean complete(Long version) {
      if (isDone()) {
        return super.complete(null);
      }
      cancel();
      if (version != null) {
        lock.set(id);
        return super.complete(version);
      } else {
        return super.complete(null);
      }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      cancel();
      return super.completeExceptionally(ex);
    }

    private void cancel() {
      if (scheduled != null) {
        scheduled.cancel();
      }
      futures.remove(id);
    }
  }
}
