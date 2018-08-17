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
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.time.Version;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Raft lock.
 */
public class AtomicLockProxy
    extends AbstractAsyncPrimitive<AsyncAtomicLock, AtomicLockService>
    implements AsyncAtomicLock, AtomicLockClient {
  private final Map<Integer, LockAttempt> attempts = Maps.newConcurrentMap();
  private final AtomicInteger id = new AtomicInteger();
  private final AtomicInteger lock = new AtomicInteger();

  public AtomicLockProxy(ProxyClient<AtomicLockService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
    proxy.addStateChangeListener(this::onStateChange);
  }

  private void onStateChange(PrimitiveState state) {
    if (state != PrimitiveState.CONNECTED) {
      for (LockAttempt attempt : attempts.values()) {
        getProxyClient().acceptBy(name(), service -> service.unlock(attempt.id()));
        attempt.completeExceptionally(new PrimitiveException.Unavailable());
      }
    }
  }

  @Override
  public void locked(int id, long version) {
    // Remove the LockAttempt from the attempts map and complete it with the lock version if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockAttempt attempt = attempts.remove(id);
    if (attempt != null) {
      attempt.complete(new Version(version));
    } else {
      getProxyClient().acceptBy(name(), service -> service.unlock(id));
    }
  }

  @Override
  public void failed(int id) {
    // Remove the LockAttempt from the attempts map and complete it with a null value if it exists.
    // If the attempt no longer exists, it likely was expired by a client-side timer.
    LockAttempt attempt = attempts.remove(id);
    if (attempt != null) {
      attempt.complete(null);
    }
  }

  @Override
  public CompletableFuture<Version> lock() {
    // Create and register a new attempt and invoke the LOCK operation on the replicated state machine.
    LockAttempt attempt = new LockAttempt();
    getProxyClient().acceptBy(name(), service -> service.lock(attempt.id(), -1)).whenComplete((result, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });
    return attempt;
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    // If the proxy is currently disconnected from the cluster, we can just fail the lock attempt here.
    PrimitiveState state = getProxyClient().getPartition(name()).getState();
    if (state != PrimitiveState.CONNECTED) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    // Create and register a new attempt and invoke the LOCK operation on teh replicated state machine with
    // a 0 timeout. The timeout will cause the state machine to immediately reject the request if the lock is
    // already owned by another process.
    LockAttempt attempt = new LockAttempt();
    getProxyClient().acceptBy(name(), service -> service.lock(attempt.id(), 0)).whenComplete((result, error) -> {
      if (error != null) {
        attempt.completeExceptionally(error);
      }
    });
    return attempt.thenApply(Optional::ofNullable);
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
      getProxyClient().acceptBy(name(), service -> service.unlock(a.id()));
    });

    // Invoke the LOCK operation on the replicated state machine with the given timeout. If the lock is currently
    // held by another process, the state machine will add the attempt to a queue and publish a FAILED event if
    // the timer expires before this process can be granted the lock. If the client cannot reach the Raft cluster,
    // the client-side timer will expire the attempt.
    getProxyClient().acceptBy(name(), service -> service.lock(attempt.id(), timeout.toMillis()))
        .whenComplete((result, error) -> {
          if (error != null) {
            attempt.completeExceptionally(error);
          }
        });
    return attempt.thenApply(Optional::ofNullable);
  }

  @Override
  public CompletableFuture<Void> unlock() {
    // Use the current lock ID to ensure we only unlock the lock currently held by this process.
    int lock = this.lock.getAndSet(0);
    if (lock != 0) {
      return getProxyClient().acceptBy(name(), service -> service.unlock(lock));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return isLocked(new Version(0));
  }

  @Override
  public CompletableFuture<Boolean> isLocked(Version version) {
    return getProxyClient().applyBy(name(), service -> service.isLocked(version.value()));
  }

  @Override
  public CompletableFuture<AsyncAtomicLock> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenApply(v -> this);
  }

  @Override
  public AtomicLock sync(Duration operationTimeout) {
    return new BlockingAtomicLock(this, operationTimeout.toMillis());
  }

  /**
   * Lock attempt.
   */
  private class LockAttempt extends CompletableFuture<Version> {
    private final int id;
    private final Scheduled scheduled;

    LockAttempt() {
      this(null, null);
    }

    LockAttempt(Duration duration, Consumer<LockAttempt> callback) {
      this.id = AtomicLockProxy.this.id.incrementAndGet();
      this.scheduled = duration != null && callback != null
          ? getProxyClient().getPartition(name()).context().schedule(duration, () -> callback.accept(this)) : null;
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
      if (scheduled != null) {
        scheduled.cancel();
      }
      attempts.remove(id);
    }
  }
}
