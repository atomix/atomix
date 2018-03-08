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
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.base.Throwables;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
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
 * Retrying Copycat session.
 */
public class RetryingRaftProxyClient extends DelegatingRaftProxyClient {
  private final Logger log;
  private final RaftProxyClient client;
  private final Scheduler scheduler;
  private final int maxRetries;
  private final Duration delayBetweenRetries;

  private final Predicate<Throwable> retryableCheck = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException
          || e instanceof RaftException.QueryFailure
          || e instanceof RaftException.UnknownClient
          || e instanceof RaftException.UnknownSession
          || e instanceof RaftException.ClosedSession
          || e instanceof RaftException.ProtocolException;

  public RetryingRaftProxyClient(RaftProxyClient delegate, Scheduler scheduler, int maxRetries, Duration delayBetweenRetries) {
    super(delegate);
    this.client = delegate;
    this.scheduler = scheduler;
    this.maxRetries = maxRetries;
    this.delayBetweenRetries = delayBetweenRetries;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftProxy.class)
        .addValue(client.sessionId())
        .add("type", client.serviceType())
        .add("name", client.name())
        .build());
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    if (getState() == RaftProxy.State.CLOSED) {
      return Futures.exceptionalFuture(new RaftException.Unavailable("Cluster is unavailable"));
    }
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    execute(operation, 1, future);
    return future;
  }

  private void execute(RaftOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
    client.execute(operation).whenComplete((r, e) -> {
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