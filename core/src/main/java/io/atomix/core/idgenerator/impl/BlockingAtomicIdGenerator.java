// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator.impl;

import io.atomix.core.idgenerator.AsyncAtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation for a {@code AtomicIdGenerator} backed by a {@link AsyncAtomicIdGenerator}.
 */
public class BlockingAtomicIdGenerator extends Synchronous<AsyncAtomicIdGenerator> implements AtomicIdGenerator {

  private final AsyncAtomicIdGenerator asyncIdGenerator;
  private final long operationTimeoutMillis;

  public BlockingAtomicIdGenerator(AsyncAtomicIdGenerator asyncIdGenerator, long operationTimeoutMillis) {
    super(asyncIdGenerator);
    this.asyncIdGenerator = asyncIdGenerator;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public long nextId() {
    return complete(asyncIdGenerator.nextId());
  }

  @Override
  public AsyncAtomicIdGenerator async() {
    return asyncIdGenerator;
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
