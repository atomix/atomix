// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session.impl;

import com.google.common.base.Throwables;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.operation.PrimitiveOperation;
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
 * Retrying primitive client.
 */
public class RetryingSessionClient extends DelegatingSessionClient {
  private final Logger log;
  private final SessionClient session;
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

  public RetryingSessionClient(SessionClient session, Scheduler scheduler, int maxRetries, Duration delayBetweenRetries) {
    super(session);
    this.session = session;
    this.scheduler = scheduler;
    this.maxRetries = maxRetries;
    this.delayBetweenRetries = delayBetweenRetries;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(this.session.sessionId())
        .add("type", this.session.type())
        .add("name", this.session.name())
        .build());
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    if (getState() == PrimitiveState.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
    }
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    execute(operation, 1, future);
    return future;
  }

  private void execute(PrimitiveOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
    session.execute(operation).whenComplete((r, e) -> {
      if (e != null) {
        if (attemptIndex < maxRetries + 1 && retryableCheck.test(Throwables.getRootCause(e))) {
          log.debug("Retry attempt ({} of {}). Failure due to {}", attemptIndex, maxRetries, Throwables.getRootCause(e).getClass());
          scheduler.schedule(delayBetweenRetries.multipliedBy(2 ^ attemptIndex), () -> execute(operation, attemptIndex + 1, future));
        } else {
          future.completeExceptionally(e);
        }
      } else {
        future.complete(r);
      }
    });
  }
}
