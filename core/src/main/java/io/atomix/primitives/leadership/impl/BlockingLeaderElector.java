/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.leadership.impl;

import io.atomix.primitives.PrimitiveException;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.leadership.AsyncLeaderElector;
import io.atomix.primitives.leadership.LeaderElector;
import io.atomix.primitives.leadership.Leadership;
import io.atomix.primitives.leadership.LeadershipEventListener;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Default implementation for a {@code LeaderElector} backed by a {@link AsyncLeaderElector}.
 */
public class BlockingLeaderElector<T> extends Synchronous<AsyncLeaderElector<T>> implements LeaderElector<T> {

  private final AsyncLeaderElector<T> asyncElector;
  private final long operationTimeoutMillis;

  public BlockingLeaderElector(AsyncLeaderElector<T> asyncElector, long operationTimeoutMillis) {
    super(asyncElector);
    this.asyncElector = asyncElector;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Leadership run(T identifier) {
    return complete(asyncElector.run(identifier));
  }

  @Override
  public void withdraw() {
    complete(asyncElector.withdraw());
  }

  @Override
  public boolean anoint(T identifier) {
    return complete(asyncElector.anoint(identifier));
  }

  @Override
  public boolean promote(T identifier) {
    return complete(asyncElector.promote(identifier));
  }

  @Override
  public void evict(T identifier) {
    complete(asyncElector.evict(identifier));
  }

  @Override
  public Leadership getLeadership() {
    return complete(asyncElector.getLeadership());
  }

  @Override
  public void addListener(LeadershipEventListener<T> listener) {
    complete(asyncElector.addListener(listener));
  }

  @Override
  public void removeListener(LeadershipEventListener<T> listener) {
    complete(asyncElector.removeListener(listener));
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    asyncElector.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    asyncElector.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return asyncElector.statusChangeListeners();
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }
}
