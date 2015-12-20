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
  private Consumer<T> subscriber;

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
    if (subscriber != null) {
      subscriber.accept((T) task);
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
   * Submits a task to the task queue.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been completed.
   */
  public CompletableFuture<Void> submit(T task) {
    return submit(new TaskQueueCommands.Submit(++taskId, task, false));
  }

  /**
   * Submits a task to the task queue and awaits processing.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been completed.
   */
  public CompletableFuture<Void> submitNow(T task) {
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
   * Subscribes to messages from the topic.
   * <p>
   * Once the returned {@link CompletableFuture} is completed, the subscriber is guaranteed to receive all
   * messages from any client thereafter. Messages are guaranteed to be received in the order specified by
   * the instance from which they were sent. The provided {@link Consumer} will always be executed on the
   * same thread.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the listener has been registered
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   topic.subscribe(message -> {
   *     ...
   *   }).join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   queue.subscribe(message -> {
   *     ...
   *   }).thenRun(() -> System.out.println("Subscribed to queue"));
   *   }
   * </pre>
   *
   * @param consumer The message consumer.
   * @return A completable future to be completed once the consumer has been subscribed.
   */
  public synchronized CompletableFuture<Listener<T>> subscribe(Consumer<T> consumer) {
    if (subscriber != null) {
      subscriber = consumer;
      return CompletableFuture.completedFuture(new TaskQueueListener(consumer));
    }
    subscriber = consumer;
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
        subscriber = null;
        submit(new TaskQueueCommands.Unsubscribe());
      }
    }
  }

}
