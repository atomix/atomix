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

import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.time.Version;

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

public class AtomicSemaphoreProxy
    extends AbstractAsyncPrimitive<AsyncAtomicSemaphore, AtomicSemaphoreService>
    implements AsyncAtomicSemaphore, AtomicSemaphoreClient {
  private static final Duration NO_TIMEOUT = Duration.ofMillis(-1);

  private final ScheduledExecutorService scheduledExecutor;
  private final Map<Long, AcquireAttempt> attempts = new ConcurrentHashMap<>();
  private final AtomicLong attemptId = new AtomicLong();

  public AtomicSemaphoreProxy(ProxyClient<AtomicSemaphoreService> proxy, PrimitiveRegistry registry, ScheduledExecutorService scheduledExecutor) {
    super(proxy, registry);
    this.scheduledExecutor = scheduledExecutor;
  }

  @Override
  public void succeeded(long id, long version, int permits) {
    AcquireAttempt attempt = attempts.remove(id);

    // If the requested is null, indicate that onExpired has been called, we just release these permits.
    if (attempt == null) {
      release(permits);
    } else {
      attempt.complete(new Version(version));
    }
  }

  @Override
  public void failed(long id) {
    onExpired(id);
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
    if (permits < 0) {
      throw new IllegalArgumentException();
    }

    long id = attemptId.incrementAndGet();
    AcquireAttempt attempt = new AcquireAttempt(id, permits, timeout, a -> onExpired(a.id()));

    attempts.put(id, attempt);
    getProxyClient().acceptBy(name(), service -> service.acquire(attempt.id(), permits, timeout.toMillis()))
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
    if (permits < 0) {
      throw new IllegalArgumentException();
    }
    return getProxyClient().acceptBy(name(), service -> service.release(permits));
  }

  @Override
  public CompletableFuture<Integer> availablePermits() {
    return getProxyClient().applyBy(name(), service -> service.available());
  }

  @Override
  public CompletableFuture<Integer> drainPermits() {
    return getProxyClient().applyBy(name(), service -> service.drain());
  }

  @Override
  public CompletableFuture<Integer> increasePermits(int permits) {
    return getProxyClient().applyBy(name(), service -> service.increase(permits));
  }

  @Override
  public CompletableFuture<Integer> reducePermits(int permits) {
    return getProxyClient().applyBy(name(), service -> service.reduce(permits));
  }

  @Override
  public CompletableFuture<QueueStatus> queueStatus() {
    return getProxyClient().applyBy(name(), service -> service.queueStatus());
  }

  @Override
  public CompletableFuture<AsyncAtomicSemaphore> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenApply(v -> this);
  }

  @Override
  public AtomicSemaphore sync(Duration operationTimeout) {
    return new BlockingAtomicSemaphore(this, operationTimeout);
  }

  /**
   * Query all permit holders.
   * For debugging only.
   *
   * @return CompletableFuture that is completed with the current permit holders
   */
  public CompletableFuture<Map<Long, Integer>> holderStatus() {
    return getProxyClient().applyBy(name(), service -> service.holderStatus());
  }

  private class AcquireAttempt extends CompletableFuture<Version> {
    private final long id;
    private final int permits;
    private ScheduledFuture<?> scheduledFuture;

    AcquireAttempt(long id, int permits) {
      this.id = id;
      this.permits = permits;
    }

    AcquireAttempt(long id, int permits, Duration timeout, Consumer<AcquireAttempt> callback) {
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
