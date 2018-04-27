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
package io.atomix.core.lock.impl;

import com.google.common.collect.Maps;
import io.atomix.core.lock.AsyncDistributedLock;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.lock.impl.DistributedLockOperations.Lock;
import io.atomix.core.lock.impl.DistributedLockOperations.Unlock;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.Proxy;
import io.atomix.utils.concurrent.OrderedExecutor;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.atomix.core.lock.impl.DistributedLockEvents.FAILED;
import static io.atomix.core.lock.impl.DistributedLockEvents.LOCKED;
import static io.atomix.core.lock.impl.DistributedLockOperations.LOCK;
import static io.atomix.core.lock.impl.DistributedLockOperations.UNLOCK;
import static io.atomix.utils.concurrent.Futures.orderedFuture;

/**
 * Raft lock.
 */
public class DistributedLockProxy extends AbstractAsyncPrimitive<AsyncDistributedLock> implements AsyncDistributedLock {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(DistributedLockOperations.NAMESPACE)
      .register(DistributedLockEvents.NAMESPACE)
      .build());

  private final ScheduledExecutorService scheduledExecutor;
  private final Executor orderedExecutor;
  private final Map<Integer, LockAttempt> attempts = Maps.newConcurrentMap();
  private final AtomicInteger id = new AtomicInteger();
  private final AtomicInteger lock = new AtomicInteger();

  public DistributedLockProxy(PrimitiveProxy proxy, PrimitiveRegistry registry, ScheduledExecutorService scheduledExecutor) {
    super(proxy, registry);
    this.scheduledExecutor = scheduledExecutor;
    this.orderedExecutor = new OrderedExecutor(scheduledExecutor);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  /**
   * Handles a {@code LOCKED} event.
   *
   * @param event the event to handle
   */
  private void handleLocked(LockEvent event) {
    // Remove the LockAttempt from the attempts map and complete it with the lock version if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockAttempt attempt = attempts.remove(event.id());
    if (attempt != null) {
      attempt.complete(new Version(event.version()));
    }
  }

  /**
   * Handles a {@code FAILED} event.
   *
   * @param event the event to handle
   */
  private void handleFailed(LockEvent event) {
    // Remove the LockAttempt from the attempts map and complete it with a null value if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockAttempt attempt = attempts.remove(event.id());
    if (attempt != null) {
      attempt.complete(null);
    }
  }

  @Override
  public CompletableFuture<Version> lock() {
    // Create and register a new attempt and invoke the LOCK operation on the replicated state machine.
    LockAttempt attempt = new LockAttempt();
    invokeBy(getPartitionKey(), LOCK, new Lock(attempt.id(), -1)).whenComplete((result, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });

    // Return an ordered future that can safely be blocked inside the executor thread.
    return orderedFuture(attempt, orderedExecutor, scheduledExecutor);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    // If the proxy is currently disconnected from the cluster, we can just fail the lock attempt here.
    Proxy.State state = getPartition(getPartitionKey()).getState();
    if (state != Proxy.State.CONNECTED) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    // Create and register a new attempt and invoke the LOCK operation on teh replicated state machine with
    // a 0 timeout. The timeout will cause the state machine to immediately reject the request if the lock is
    // already owned by another process.
    LockAttempt attempt = new LockAttempt();
    invokeBy(getPartitionKey(), LOCK, new Lock(attempt.id(), 0)).whenComplete((result, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });

    // Return an ordered future that can safely be blocked inside the executor thread.
    return orderedFuture(attempt, orderedExecutor, scheduledExecutor)
        .thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock(Duration timeout) {
    // Create a lock attempt with a client-side timeout and fail the lock if the timer expires.
    // Because time does not progress at the same rate on different nodes, we can't guarantee that
    // the lock won't be granted to this process after it's expired here. Thus, if this timer expires and
    // we fail the lock on the client, we also still need to send an UNLOCK command to the cluster in case it's
    // later granted by the cluster. Note that the semantics of the Raft client will guarantee this operation
    // occurs after any prior LOCK attempt, and the Raft client will retry the UNLOCK request until successful.
    // Additionally, sending the unique lock ID with the command ensures we won't accidentally unlock a different
    // lock call also granted to this process.
    LockAttempt attempt = new LockAttempt(timeout, a -> {
      a.complete(null);
      invokeBy(getPartitionKey(), UNLOCK, new Unlock(a.id()));
    });

    // Invoke the LOCK operation on the replicated state machine with the given timeout. If the lock is currently
    // held by another process, the state machine will add the attempt to a queue and publish a FAILED event if
    // the timer expires before this process can be granted the lock. If the client cannot reach the Raft cluster,
    // the client-side timer will expire the attempt.
    invokeBy(getPartitionKey(), LOCK, new Lock(attempt.id(), timeout.toMillis()))
        .whenComplete((result, error) -> {
          if (error != null) {
            attempt.completeExceptionally(error);
          }
        });

    // Return an ordered future that can safely be blocked inside the executor thread.
    return orderedFuture(attempt, orderedExecutor, scheduledExecutor)
        .thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Void> unlock() {
    // Use the current lock ID to ensure we only unlock the lock currently held by this process.
    int lock = this.lock.getAndSet(0);
    if (lock != 0) {
      return orderedFuture(
          invokeBy(getPartitionKey(), UNLOCK, new Unlock(lock)),
          orderedExecutor,
          scheduledExecutor);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncDistributedLock> connect() {
    return super.connect()
        .thenCompose(v -> getPartition(getPartitionKey()).connect())
        .thenRun(() -> {
          listenBy(getPartitionKey(), LOCKED, this::handleLocked);
          listenBy(getPartitionKey(), FAILED, this::handleFailed);
        }).thenApply(v -> this);
  }

  @Override
  public DistributedLock sync(Duration operationTimeout) {
    return new BlockingDistributedLock(this, operationTimeout.toMillis());
  }

  /**
   * Lock attempt.
   */
  private class LockAttempt extends CompletableFuture<Version> {
    private final int id;
    private final ScheduledFuture<?> scheduledFuture;

    LockAttempt() {
      this(null, null);
    }

    LockAttempt(Duration duration, Consumer<LockAttempt> callback) {
      this.id = DistributedLockProxy.this.id.incrementAndGet();
      this.scheduledFuture = duration != null && callback != null
          ? scheduledExecutor.schedule(() -> callback.accept(this), duration.toMillis(), TimeUnit.MILLISECONDS)
          : null;
      attempts.put(id, this);
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
    public boolean complete(Version version) {
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
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
      }
      attempts.remove(id);
    }
  }
}
