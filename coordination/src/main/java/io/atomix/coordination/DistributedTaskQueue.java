/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.coordination;

import io.atomix.catalyst.util.Listener;
import io.atomix.coordination.state.TaskQueueCommands;
import io.atomix.coordination.state.TaskQueueState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.Consistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.resource.ResourceTypeInfo;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Distributed task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-25, stateMachine=TaskQueueState.class)
public class DistributedTaskQueue<T> extends Resource<DistributedTaskQueue<T>> {
  public static final ResourceType<DistributedTaskQueue> TYPE = new ResourceType<>(DistributedTaskQueue.class);
  private long taskId;
  private final Map<Long, CompletableFuture<Void>> taskFutures = new ConcurrentHashMap<>();
  private Consumer<T> consumer;

  @SuppressWarnings("unchecked")
  public DistributedTaskQueue(CopycatClient client) {
    super(client);
    client.session().onEvent("process", this::process);
    client.session().onEvent("ack", this::ack);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ResourceType<DistributedTaskQueue<T>> type() {
    return (ResourceType) TYPE;
  }

  /**
   * Begins processing tasks.
   */
  @SuppressWarnings("unchecked")
  private void process(T task) {
    if (consumer != null) {
      consumer.accept(task);
      submit(new TaskQueueCommands.Ack()).whenComplete((result, error) -> {
        if (error == null && result != null) {
          process((T) result);
        }
      });
    }
  }

  /**
   * Handles a task acknowledgement.
   */
  private void ack(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.complete(null);
    }
  }

  /**
   * Sets the queue to synchronous mode.
   * <p>
   * Setting the queue to synchronous mode effectively configures the queue's {@link Consistency} to
   * {@link Consistency#ATOMIC}. Atomic consistency means that tasks {@link #submit(Object) submitted} to the
   * queue will be received and processed by a {@link #consumer(Consumer) consumer} some time between the
   * invocation of the submit operation and its completion. In other words, synchronous task queues await
   * acknowledgement from a consumer.
   *
   * @return The distributed task queue.
   */
  public DistributedTaskQueue<T> sync() {
    return with(Consistency.ATOMIC);
  }

  /**
   * Sets the queue to asynchronous mode.
   * <p>
   * Setting the queue to asynchronous mode effectively configures the queue's {@link Consistency} to
   * {@link Consistency#SEQUENTIAL}. Sequential consistency means that once a task is {@link #submit(Object) submitted}
   * to the queue, the task will be persisted in the cluster but may be delivered to a {@link #consumer(Consumer) consumer}
   * after some arbitrary delay. Tasks are guaranteed to be delivered to consumers in the order in which they were sent
   * (sequential consistency) but different consumers may receive different tasks at different points in time.
   *
   * @return The distributed task queue.
   */
  public DistributedTaskQueue<T> async() {
    return with(Consistency.SEQUENTIAL);
  }

  /**
   * Submits a task to the task queue.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been completed.
   */
  public CompletableFuture<Void> submit(T task) {
    if (consistency() == Consistency.ATOMIC) {
      return submitAtomic(task);
    }
    return submitSequential(task);
  }

  /**
   * Submits a task to the task queue and awaits processing.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been completed.
   */
  private CompletableFuture<Void> submitAtomic(T task) {
    long taskId = ++this.taskId;
    CompletableFuture<Void> future = new CompletableFuture<>();
    taskFutures.put(taskId, future);
    submit(new TaskQueueCommands.Submit(taskId, task, true)).whenComplete((result, error) -> {
      if (error != null) {
        taskFutures.remove(taskId);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Submits a task to the task queue without awaiting processing.
   */
  private CompletableFuture<Void> submitSequential(T task) {
    return submit(new TaskQueueCommands.Submit(++taskId, task, false));
  }

  /**
   * Registers a task consumer.
   * <p>
   * Once the returned {@link CompletableFuture} is completed, the consumer will begin consuming tasks
   * from the queue. The task queue guarantees that only one consumer will fully process any given task.
   * However, failures during the processing of a task will result in the task being reprocessed by another
   * consumer. Consumers should be idempotent to account for failures.
   * <p>
   * All tasks are guaranteed to be received on the same thread and tasks will be received in the order in
   * which they were submitted to the queue. Once the consumer callback has exited, the task will be acknowledged
   * and the next task fetched from the cluster.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the consumer has been registered
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   topic.consume(task -> {
   *     ...
   *   }).join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   queue.consume(task -> {
   *     ...
   *   }).thenRun(() -> System.out.println("Consumer registered"));
   *   }
   * </pre>
   *
   * @param consumer The task consumer.
   * @return A completable future to be completed once the consumer has been registered.
   */
  public synchronized CompletableFuture<Listener<T>> consumer(Consumer<T> consumer) {
    if (this.consumer != null) {
      this.consumer = consumer;
      return CompletableFuture.completedFuture(new TaskQueueListener(consumer));
    }
    this.consumer = consumer;
    return submit(new TaskQueueCommands.Subscribe())
      .thenApply(v -> new TaskQueueListener(consumer));
  }

  /**
   * Task queue listener.
   */
  private class TaskQueueListener implements Listener<T> {
    private final Consumer<T> listener;

    private TaskQueueListener(Consumer<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T message) {
      listener.accept(message);
    }

    @Override
    public void close() {
      synchronized (DistributedTaskQueue.this) {
        consumer = null;
        submit(new TaskQueueCommands.Unsubscribe());
      }
    }
  }

}
