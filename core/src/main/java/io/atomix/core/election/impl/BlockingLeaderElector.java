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
package io.atomix.core.election.impl;

import com.google.common.base.Throwables;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElector;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.Synchronous;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Default implementation for a {@code LeaderElector} backed by a {@link AsyncLeaderElection}.
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
  public Leadership<T> run(String topic, T identifier) {
    return complete(asyncElector.run(topic, identifier));
  }

  @Override
  public void withdraw(String topic, T identifier) {
    complete(asyncElector.withdraw(topic, identifier));
  }

  @Override
  public boolean anoint(String topic, T identifier) {
    return complete(asyncElector.anoint(topic, identifier));
  }

  @Override
  public boolean promote(String topic, T identifier) {
    return complete(asyncElector.promote(topic, identifier));
  }

  @Override
  public void evict(T identifier) {
    complete(asyncElector.evict(identifier));
  }

  @Override
  public Leadership<T> getLeadership(String topic) {
    return complete(asyncElector.getLeadership(topic));
  }

  @Override
  public Map<String, Leadership<T>> getLeaderships() {
    return complete(asyncElector.getLeaderships());
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
  public void addListener(String topic, LeadershipEventListener<T> listener) {
    complete(asyncElector.addListener(topic, listener));
  }

  @Override
  public void removeListener(String topic, LeadershipEventListener<T> listener) {
    complete(asyncElector.removeListener(topic, listener));
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncElector.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncElector.removeStateChangeListener(listener);
  }

  @Override
  public AsyncLeaderElector<T> async() {
    return asyncElector;
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
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
