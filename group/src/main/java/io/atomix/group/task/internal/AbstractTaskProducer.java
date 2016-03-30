/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.group.task.internal;

import io.atomix.group.internal.GroupCommands;
import io.atomix.group.task.TaskFailedException;
import io.atomix.group.task.TaskProducer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract task producer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractTaskProducer<T> implements TaskProducer<T> {
  protected final String name;
  protected volatile Options options;
  protected final AbstractTaskClient client;
  private long taskId;
  private final Map<Long, CompletableFuture<Void>> taskFutures = new ConcurrentHashMap<>();

  protected AbstractTaskProducer(String name, Options options, AbstractTaskClient client) {
    this.name = name;
    this.options = options;
    this.client = client;
  }

  /**
   * Returns the producer name.
   *
   * @return The producer name.
   */
  String name() {
    return name;
  }

  /**
   * Sets the producer options.
   */
  void setOptions(Options options) {
    this.options = options;
  }

  /**
   * Called when a task is acknowledged.
   *
   * @param taskId The task ID.
   */
  public void onAck(long taskId) {
    CompletableFuture<Void> taskFuture = taskFutures.remove(taskId);
    if (taskFuture != null) {
      taskFuture.complete(null);
    }
  }

  /**
   * Called when a task is failed.
   *
   * @param taskId The task ID.
   */
  public void onFail(long taskId) {
    CompletableFuture<Void> taskFuture = taskFutures.remove(taskId);
    if (taskFuture != null) {
      taskFuture.completeExceptionally(new TaskFailedException("task failed"));
    }
  }

  /**
   * Submits the task to the given member.
   */
  protected CompletableFuture<Void> submit(String member, T task) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final long taskId = ++this.taskId;
    taskFutures.put(taskId, future);
    client.submitter().submit(new GroupCommands.Submit(member, name, taskId, task)).whenComplete((result, error) -> {
      if (error != null) {
        CompletableFuture<Void> taskFuture = taskFutures.remove(taskId);
        if (taskFuture != null) {
          taskFuture.completeExceptionally(error);
        }
      }
    });
    return future;
  }

  @Override
  public void close() {
    client.close(this);
  }

}
