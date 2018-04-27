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
package io.atomix.core.queue.impl;

import com.google.common.collect.ImmutableList;
import io.atomix.core.queue.AsyncWorkQueue;
import io.atomix.core.queue.Task;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.queue.WorkQueueStats;
import io.atomix.core.queue.impl.WorkQueueOperations.Add;
import io.atomix.core.queue.impl.WorkQueueOperations.Complete;
import io.atomix.core.queue.impl.WorkQueueOperations.Take;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.Proxy;
import io.atomix.utils.concurrent.AbstractAccumulator;
import io.atomix.utils.concurrent.Accumulator;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.atomix.core.queue.impl.WorkQueueEvents.TASK_AVAILABLE;
import static io.atomix.core.queue.impl.WorkQueueOperations.ADD;
import static io.atomix.core.queue.impl.WorkQueueOperations.CLEAR;
import static io.atomix.core.queue.impl.WorkQueueOperations.COMPLETE;
import static io.atomix.core.queue.impl.WorkQueueOperations.REGISTER;
import static io.atomix.core.queue.impl.WorkQueueOperations.STATS;
import static io.atomix.core.queue.impl.WorkQueueOperations.TAKE;
import static io.atomix.core.queue.impl.WorkQueueOperations.UNREGISTER;
import static io.atomix.utils.concurrent.Threads.namedThreads;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Distributed resource providing the {@link WorkQueue} primitive.
 */
public class WorkQueueProxy extends AbstractAsyncPrimitive<AsyncWorkQueue<byte[]>> implements AsyncWorkQueue<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(WorkQueueOperations.NAMESPACE)
      .register(WorkQueueEvents.NAMESPACE)
      .build());

  private final Logger log = getLogger(getClass());
  private final ExecutorService executor;
  private final AtomicReference<TaskProcessor> taskProcessor = new AtomicReference<>();
  private final Timer timer = new Timer("atomix-work-queue-completer");
  private final AtomicBoolean isRegistered = new AtomicBoolean(false);

  public WorkQueueProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
    executor = newSingleThreadExecutor(namedThreads("atomix-work-queue-" + proxy.name() + "-%d", log));
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public CompletableFuture<Void> delete() {
    executor.shutdown();
    timer.cancel();
    return invokeBy(getPartitionKey(), CLEAR);
  }

  @Override
  public CompletableFuture<Void> addMultiple(Collection<byte[]> items) {
    if (items.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return invokeBy(getPartitionKey(), ADD, new Add(items));
  }

  @Override
  public CompletableFuture<Collection<Task<byte[]>>> take(int maxTasks) {
    if (maxTasks <= 0) {
      return CompletableFuture.completedFuture(ImmutableList.of());
    }
    return invokeBy(getPartitionKey(), TAKE, new Take(maxTasks));
  }

  @Override
  public CompletableFuture<Void> complete(Collection<String> taskIds) {
    if (taskIds.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return invokeBy(getPartitionKey(), COMPLETE, new Complete(taskIds));
  }

  @Override
  public CompletableFuture<Void> registerTaskProcessor(Consumer<byte[]> callback,
                                                       int parallelism,
                                                       Executor executor) {
    Accumulator<String> completedTaskAccumulator =
        new CompletedTaskAccumulator(timer, 50, 50); // TODO: make configurable
    taskProcessor.set(new TaskProcessor(callback,
        parallelism,
        executor,
        completedTaskAccumulator));
    return register().thenCompose(v -> take(parallelism))
        .thenAccept(taskProcessor.get());
  }

  @Override
  public CompletableFuture<Void> stopProcessing() {
    return unregister();
  }

  @Override
  public CompletableFuture<WorkQueueStats> stats() {
    return invokeBy(getPartitionKey(), STATS);
  }

  private void resumeWork() {
    TaskProcessor activeProcessor = taskProcessor.get();
    if (activeProcessor == null) {
      return;
    }
    this.take(activeProcessor.headRoom())
        .whenCompleteAsync((tasks, e) -> activeProcessor.accept(tasks), executor);
  }

  private CompletableFuture<Void> register() {
    return invokeBy(getPartitionKey(), REGISTER).thenRun(() -> isRegistered.set(true));
  }

  private CompletableFuture<Void> unregister() {
    return invokeBy(getPartitionKey(), UNREGISTER).thenRun(() -> isRegistered.set(false));
  }

  @Override
  public CompletableFuture<AsyncWorkQueue<byte[]>> connect() {
    return super.connect()
        .thenRun(() -> {
          addStateChangeListenerBy(getPartitionKey(), state -> {
            if (state == Proxy.State.CONNECTED && isRegistered.get()) {
              invokeBy(getPartitionKey(), REGISTER);
            }
          });
          listenBy(getPartitionKey(), TASK_AVAILABLE, this::resumeWork);
        }).thenApply(v -> this);
  }

  @Override
  public WorkQueue<byte[]> sync(Duration operationTimeout) {
    return new BlockingWorkQueue<>(this, operationTimeout.toMillis());
  }

  // TaskId accumulator for paced triggering of task completion calls.
  private class CompletedTaskAccumulator extends AbstractAccumulator<String> {
    CompletedTaskAccumulator(Timer timer, int maxTasksToBatch, int maxBatchMillis) {
      super(timer, maxTasksToBatch, maxBatchMillis, Integer.MAX_VALUE);
    }

    @Override
    public void processItems(List<String> items) {
      complete(items);
    }
  }

  private class TaskProcessor implements Consumer<Collection<Task<byte[]>>> {

    private final AtomicInteger headRoom;
    private final Consumer<byte[]> backingConsumer;
    private final Executor executor;
    private final Accumulator<String> taskCompleter;

    public TaskProcessor(Consumer<byte[]> backingConsumer,
                         int parallelism,
                         Executor executor,
                         Accumulator<String> taskCompleter) {
      this.backingConsumer = backingConsumer;
      this.headRoom = new AtomicInteger(parallelism);
      this.executor = executor;
      this.taskCompleter = taskCompleter;
    }

    public int headRoom() {
      return headRoom.get();
    }

    @Override
    public void accept(Collection<Task<byte[]>> tasks) {
      if (tasks == null) {
        return;
      }
      headRoom.addAndGet(-1 * tasks.size());
      tasks.forEach(task ->
          executor.execute(() -> {
            try {
              backingConsumer.accept(task.payload());
              taskCompleter.add(task.taskId());
            } catch (Exception e) {
              log.debug("Task execution failed", e);
            } finally {
              headRoom.incrementAndGet();
              resumeWork();
            }
          }));
    }
  }
}