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

import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Acquire;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Drain;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Increase;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Reduce;
import io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.Release;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Version;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.ACQUIRE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.AVAILABLE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.DRAIN;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.HOLDER_STATUS;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.INCREASE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.QUEUE_STATUS;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.REDUCE;
import static io.atomix.core.semaphore.impl.DistributedSemaphoreOperations.RELEASE;

public class DistributedSemaphoreProxy extends AbstractAsyncPrimitive<AsyncDistributedSemaphore> implements AsyncDistributedSemaphore {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .register(DistributedSemaphoreOperations.NAMESPACE)
          .register(DistributedSemaphoreEvents.NAMESPACE)
          .register(QueueStatus.class)
          .register(Versioned.class)
          .build());
  private static Duration NO_TIMEOUT = Duration.ofMillis(-1);

  private final ScheduledExecutorService scheduledExecutor;
  private final Map<Long, AcquireAttempt> attempts = new ConcurrentHashMap<>();
  private final AtomicLong operationId = new AtomicLong();

  public DistributedSemaphoreProxy(PrimitiveProxy proxy,
                                   PrimitiveRegistry registry,
                                   ScheduledExecutorService scheduledExecutor) {
    super(proxy, registry);
    this.scheduledExecutor = scheduledExecutor;
  }

  @Override
  public CompletableFuture<AsyncDistributedSemaphore> connect() {
    return super.connect()
            .thenCompose(v -> getPartition(getPartitionKey()).connect())
            .thenRun(() -> {
              listenBy(getPartitionKey(), DistributedSemaphoreEvents.SUCCESS, this::onSucceeded);
              listenBy(getPartitionKey(), DistributedSemaphoreEvents.FAILED, this::onFailed);
            })
            .thenApply(v -> this);
  }

  private void onSucceeded(SemaphoreEvent event) {
    AcquireAttempt attempt = attempts.remove(event.id());

    // If the requested is null, indicate that onExpired has been called, we just release these permits.
    if (attempt == null) {
      release(event.permits());
    } else {
      attempt.complete(new Version(event.version()));
    }
  }

  private void onFailed(SemaphoreEvent event) {
    onExpired(event.id());
  }

  private void onExpired(long id) {
    AcquireAttempt attempt = attempts.remove(id);
    if (attempt != null) {
      attempt.complete(null);
    }
  }

  @Override
  public CompletableFuture<Version> acquire() {
    return acquire(1);
  }

  @Override
  public CompletableFuture<Version> acquire(int permits) {
    return tryAcquire(permits, NO_TIMEOUT).thenApply(Optional::get);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryAcquire() {
    return tryAcquire(1);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryAcquire(int permits) {
    return tryAcquire(permits, Duration.ZERO);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryAcquire(Duration timeout) {
    return tryAcquire(1, timeout);
  }

  @Override
  public CompletableFuture<Optional<Version>> tryAcquire(int permits, Duration timeout) {
    if (permits < 0) throw new IllegalArgumentException();

    long id = operationId.incrementAndGet();
    AcquireAttempt attempt = new AcquireAttempt(id, permits, timeout, a -> onExpired(a.id()));

    attempts.put(id, attempt);
    invokeBy(getPartitionKey(), ACQUIRE, new Acquire(attempt.id(), permits, timeout.toMillis()))
            .whenComplete((result, error) -> {
              if (error != null) {
                attempts.remove(id);
                attempt.complete(null);
              }
            });
    return attempt.thenApply(Optional::ofNullable);
  }


  @Override
  public CompletableFuture<Void> release() {
    return release(1);
  }

  @Override
  public CompletableFuture<Void> release(int permits) {
    if (permits < 0) throw new IllegalArgumentException();
    return invokeBy(getPartitionKey(), RELEASE, new Release(operationId.incrementAndGet(), permits));
  }

  @Override
  public CompletableFuture<Versioned<Integer>> availablePermits() {
    return invokeBy(getPartitionKey(), AVAILABLE);
  }

  @Override
  public CompletableFuture<Versioned<Integer>> drainPermits() {
    return invokeBy(getPartitionKey(), DRAIN, new Drain(operationId.incrementAndGet()));
  }

  @Override
  public CompletableFuture<Versioned<Integer>> increase(int permits) {
    return invokeBy(getPartitionKey(), INCREASE, new Increase(operationId.incrementAndGet(), permits)
    );
  }

  @Override
  public CompletableFuture<Versioned<Integer>> reduce(int permits) {
    return invokeBy(getPartitionKey(), REDUCE, new Reduce(operationId.incrementAndGet(), permits));
  }

  @Override
  public CompletableFuture<Versioned<QueueStatus>> queueStatus() {
    return invokeBy(getPartitionKey(), QUEUE_STATUS);
  }

  @Override
  public DistributedSemaphore sync(Duration operationTimeout) {
    return new BlockingDistributedSemaphore(this, operationTimeout);
  }

  /**
   * Query all permit holders.
   * For debugging only.
   *
   * @return CompletableFuture that is completed with the current permit holders
   */
  CompletableFuture<Versioned<Map<Long, Integer>>> holderStatus() {
    return invokeBy(getPartitionKey(), HOLDER_STATUS);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  private class AcquireAttempt extends CompletableFuture<Version> {
    private final long id;
    private final int permits;
    private ScheduledFuture<?> scheduledFuture;

    public AcquireAttempt(long id, int permits) {
      this.id = id;
      this.permits = permits;
    }

    public AcquireAttempt(long id, int permits, Duration timeout, Consumer<AcquireAttempt> callback) {
      this(id, permits);
      this.scheduledFuture = timeout != null && callback != null && timeout.toMillis() > 0
              ? scheduledExecutor.schedule(() -> callback.accept(this), timeout.toMillis(), TimeUnit.MILLISECONDS)
              : null;
    }

    public long id() {
      return id;
    }

    public int permits() {
      return permits;
    }

    @Override
    public boolean complete(Version version) {
      if (isDone()) {
        return super.complete(null);
      }
      cancel();
      if (version != null) {
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
    }
  }
}
