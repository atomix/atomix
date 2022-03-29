// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session.impl;

import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default session ID service.
 */
public class DefaultSessionIdService implements ManagedSessionIdService {
  private final Random random = new Random();
  private final AtomicBoolean started = new AtomicBoolean();

  @Override
  public CompletableFuture<SessionId> nextSessionId() {
    return CompletableFuture.completedFuture(SessionId.from(random.nextLong()));
  }

  @Override
  public CompletableFuture<SessionIdService> start() {
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
