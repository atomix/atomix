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
package io.atomix.primitives.queue.impl;

import com.google.common.collect.ImmutableList;
import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.primitives.queue.AsyncWorkQueue;
import io.atomix.primitives.queue.Task;
import io.atomix.primitives.queue.WorkQueue;
import io.atomix.primitives.queue.WorkQueueStats;
import io.atomix.primitives.queue.impl.RaftWorkQueueOperations.Add;
import io.atomix.primitives.queue.impl.RaftWorkQueueOperations.Complete;
import io.atomix.primitives.queue.impl.RaftWorkQueueOperations.Take;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;
import io.atomix.utils.AbstractAccumulator;
import io.atomix.utils.Accumulator;
import org.slf4j.Logger;

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

import static io.atomix.primitives.queue.impl.RaftWorkQueueEvents.TASK_AVAILABLE;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.ADD;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.CLEAR;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.COMPLETE;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.REGISTER;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.STATS;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.TAKE;
import static io.atomix.primitives.queue.impl.RaftWorkQueueOperations.UNREGISTER;
import static io.atomix.utils.concurrent.Threads.namedThreads;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Distributed resource providing the {@link WorkQueue} primitive.
 */
public class RaftWorkQueue extends AbstractRaftPrimitive implements AsyncWorkQueue<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftWorkQueueOperations.NAMESPACE)
      .register(RaftWorkQueueEvents.NAMESPACE)
      .build());

  private final Logger log = getLogger(getClass());
  private final ExecutorService executor;
  private final AtomicReference<TaskProcessor> taskProcessor = new AtomicReference<>();
  private final Timer timer = new Timer("atomix-work-queue-completer");
  private final AtomicBoolean isRegistered = new AtomicBoolean(false);

  public RaftWorkQueue(RaftProxy proxy) {
    super(proxy);
    executor = newSingleThreadExecutor(namedThreads("atomix-work-queue-" + proxy.name() + "-%d", log));
    proxy.addStateChangeListener(state -> {
      if (state == RaftProxy.State.CONNECTED && isRegistered.get()) {
        proxy.invoke(REGISTER);
      }
    });
    proxy.addEventListener(TASK_AVAILABLE, this::resumeWork);
  }

  @Override
  public CompletableFuture<Void> destroy() {
    executor.shutdown();
    timer.cancel();
    return proxy.invoke(CLEAR);
  }

  @Override
  public CompletableFuture<Void> addMultiple(Collection<byte[]> items) {
    if (items.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return proxy.invoke(ADD, SERIALIZER::encode, new Add(items));
  }

  @Override
  public CompletableFuture<Collection<Task<byte[]>>> take(int maxTasks) {
    if (maxTasks <= 0) {
      return CompletableFuture.completedFuture(ImmutableList.of());
    }
    return proxy.invoke(TAKE, SERIALIZER::encode, new Take(maxTasks), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> complete(Collection<String> taskIds) {
    if (taskIds.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return proxy.invoke(COMPLETE, SERIALIZER::encode, new Complete(taskIds));
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
    return proxy.invoke(STATS, SERIALIZER::decode);
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
    return proxy.invoke(REGISTER).thenRun(() -> isRegistered.set(true));
  }

  private CompletableFuture<Void> unregister() {
    return proxy.invoke(UNREGISTER).thenRun(() -> isRegistered.set(false));
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