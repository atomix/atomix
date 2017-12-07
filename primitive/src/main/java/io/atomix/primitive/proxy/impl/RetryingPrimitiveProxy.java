/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.proxy.impl;

import com.google.common.base.Throwables;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduler;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Retrying primitive proxy.
 */
public class RetryingPrimitiveProxy extends DelegatingPrimitiveProxy {
  private final Logger log;
  private final PrimitiveProxy proxy;
  private final Scheduler scheduler;
  private final int maxRetries;
  private final Duration delayBetweenRetries;

  private final Predicate<Throwable> retryableCheck = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException
          || e instanceof PrimitiveException.Unavailable
          || e instanceof PrimitiveException.Timeout
          || e instanceof PrimitiveException.QueryFailure
          || e instanceof PrimitiveException.UnknownClient
          || e instanceof PrimitiveException.UnknownSession
          || e instanceof PrimitiveException.ClosedSession;

  public RetryingPrimitiveProxy(PrimitiveProxy delegate, Scheduler scheduler, int maxRetries, Duration delayBetweenRetries) {
    super(delegate);
    this.proxy = delegate;
    this.scheduler = scheduler;
    this.maxRetries = maxRetries;
    this.delayBetweenRetries = delayBetweenRetries;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveProxy.class)
        .addValue(proxy.sessionId())
        .add("type", proxy.serviceType())
        .add("name", proxy.name())
        .build());
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    if (getState() == PrimitiveProxy.State.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.Unavailable());
    }
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    execute(operation, 1, future);
    return future;
  }

  private void execute(PrimitiveOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
    proxy.execute(operation).whenComplete((r, e) -> {
      if (e != null) {
        if (attemptIndex < maxRetries + 1 && retryableCheck.test(Throwables.getRootCause(e))) {
          log.debug("Retry attempt ({} of {}). Failure due to {}", attemptIndex, maxRetries, Throwables.getRootCause(e).getClass());
          scheduleRetry(operation, attemptIndex, future);
        } else {
          future.completeExceptionally(e);
        }
      } else {
        future.complete(r);
      }
    });
  }

  private void scheduleRetry(PrimitiveOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
    PrimitiveProxy.State retryState = proxy.getState();
    scheduler.schedule(delayBetweenRetries, () -> {
      if (retryState == PrimitiveProxy.State.CONNECTED || proxy.getState() == PrimitiveProxy.State.CONNECTED) {
        execute(operation, attemptIndex + 1, future);
      } else {
        scheduleRetry(operation, attemptIndex, future);
      }
    });
  }
}