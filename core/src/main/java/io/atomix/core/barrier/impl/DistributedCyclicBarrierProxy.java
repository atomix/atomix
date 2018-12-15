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
package io.atomix.core.barrier.impl;

import io.atomix.core.barrier.AsyncDistributedCyclicBarrier;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed cyclic barrier proxy.
 */
public class DistributedCyclicBarrierProxy
    extends AbstractAsyncPrimitive<AsyncDistributedCyclicBarrier, DistributedCyclicBarrierService>
    implements AsyncDistributedCyclicBarrier, DistributedCyclicBarrierClient {

  private final Runnable barrierAction;
  private volatile CompletableFuture<Integer> awaitFuture;
  private volatile long barrierId;

  public DistributedCyclicBarrierProxy(ProxyClient<DistributedCyclicBarrierService> client, PrimitiveRegistry registry, Runnable barrierAction) {
    super(client, registry);
    this.barrierAction = barrierAction;
  }

  @Override
  public synchronized void broken(long id) {
    if (barrierId == id) {
      CompletableFuture<Integer> awaitFuture = this.awaitFuture;
      if (awaitFuture != null) {
        awaitFuture.completeExceptionally(new BrokenBarrierException());
      }
      this.barrierId = 0;
      this.awaitFuture = null;
    }
  }

  @Override
  public synchronized void release(long id, int index) {
    if (barrierId == id) {
      CompletableFuture<Integer> awaitFuture = this.awaitFuture;
      if (awaitFuture != null) {
        awaitFuture.complete(index);
      }
      this.barrierId = 0;
      this.awaitFuture = null;
    }
  }

  @Override
  public void runAction() {
    barrierAction.run();
  }

  @Override
  public CompletableFuture<Integer> await() {
    return await(Duration.ofMillis(0));
  }

  @Override
  public synchronized CompletableFuture<Integer> await(Duration timeout) {
    if (awaitFuture == null) {
      CompletableFuture<Integer> awaitFuture = new CompletableFuture<>();
      this.awaitFuture = awaitFuture;
      getProxyClient().applyBy(name(), service -> service.await(timeout.toMillis()))
          .thenAccept(result -> {
            if (result.status() == CyclicBarrierResult.Status.OK) {
              this.barrierId = result.result();
            } else if (result.status() == CyclicBarrierResult.Status.BROKEN) {
              synchronized (this) {
                this.awaitFuture = null;
              }
              awaitFuture.completeExceptionally(new BrokenBarrierException());
            }
          });
    }
    return awaitFuture;
  }

  @Override
  public CompletableFuture<Integer> getNumberWaiting() {
    return getProxyClient().applyBy(name(), service -> service.getNumberWaiting());
  }

  @Override
  public CompletableFuture<Integer> getParties() {
    return getProxyClient().applyBy(name(), service -> service.getParties());
  }

  @Override
  public CompletableFuture<Boolean> isBroken() {
    return getProxyClient().applyBy(name(), service -> service.isBroken(barrierId));
  }

  @Override
  public CompletableFuture<Void> reset() {
    return getProxyClient().acceptBy(name(), service -> service.reset(barrierId));
  }

  @Override
  public CompletableFuture<AsyncDistributedCyclicBarrier> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenCompose(v -> getProxyClient().acceptBy(name(), service -> service.join()))
        .thenApply(v -> this);
  }

  @Override
  public DistributedCyclicBarrier sync(Duration operationTimeout) {
    return new BlockingDistributedCyclicBarrier(this, operationTimeout.toMillis());
  }
}
