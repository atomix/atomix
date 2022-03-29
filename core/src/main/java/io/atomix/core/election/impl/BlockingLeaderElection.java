// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election.impl;

import com.google.common.base.Throwables;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.Synchronous;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Default implementation for a {@code LeaderElector} backed by a {@link AsyncLeaderElection}.
 */
public class BlockingLeaderElection<T> extends Synchronous<AsyncLeaderElection<T>> implements LeaderElection<T> {

  private final AsyncLeaderElection<T> asyncElector;
  private final long operationTimeoutMillis;

  public BlockingLeaderElection(AsyncLeaderElection<T> asyncElector, long operationTimeoutMillis) {
    super(asyncElector);
    this.asyncElector = asyncElector;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Leadership<T> run(T identifier) {
    return complete(asyncElector.run(identifier));
  }

  @Override
  public void withdraw(T identifier) {
    complete(asyncElector.withdraw(identifier));
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
  public Leadership<T> getLeadership() {
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
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncElector.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncElector.removeStateChangeListener(listener);
  }

  @Override
  public AsyncLeaderElection<T> async() {
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
