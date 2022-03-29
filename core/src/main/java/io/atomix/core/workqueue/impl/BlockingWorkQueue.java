// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue.impl;

import com.google.common.base.Throwables;
import io.atomix.core.workqueue.AsyncWorkQueue;
import io.atomix.core.workqueue.Task;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.core.workqueue.WorkQueueStats;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Default synchronous work queue implementation.
 */
public class BlockingWorkQueue<E> extends Synchronous<AsyncWorkQueue<E>> implements WorkQueue<E> {

  private final AsyncWorkQueue<E> asyncQueue;
  private final long operationTimeoutMillis;

  public BlockingWorkQueue(AsyncWorkQueue<E> asyncQueue, long operationTimeoutMillis) {
    super(asyncQueue);
    this.asyncQueue = asyncQueue;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public void addMultiple(Collection<E> items) {
    complete(asyncQueue.addMultiple(items));
  }

  @Override
  public Collection<Task<E>> take(int maxItems) {
    return complete(asyncQueue.take(maxItems));
  }

  @Override
  public void complete(Collection<String> taskIds) {
    complete(asyncQueue.complete(taskIds));
  }

  @Override
  public void registerTaskProcessor(Consumer<E> taskProcessor, int parallelism, Executor executor) {
    complete(asyncQueue.registerTaskProcessor(taskProcessor, parallelism, executor));
  }

  @Override
  public void stopProcessing() {
    complete(asyncQueue.stopProcessing());
  }

  @Override
  public WorkQueueStats stats() {
    return complete(asyncQueue.stats());
  }

  @Override
  public AsyncWorkQueue<E> async() {
    return asyncQueue;
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
