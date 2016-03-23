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
package io.atomix.group.tasks;

import io.atomix.catalyst.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Task queue controller.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TaskQueueController {
  private final TaskQueue queue;

  public TaskQueueController(TaskQueue queue) {
    this.queue = Assert.notNull(queue, "queue");
  }

  /**
   * Returns the underlying task queue.
   *
   * @return The underlying task queue.
   */
  public TaskQueue queue() {
    return queue;
  }

  /**
   * Called when a task is received.
   *
   * @param task The task that was received.
   * @return A completable future to be completed once the task is acknowledged or failed.
   */
  public CompletableFuture<Boolean> onTask(Task<?> task) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    ((LocalTaskQueue) queue).onTask(task.setFuture(future));
    return future;
  }

  /**
   * Called when a task is acknowledged.
   *
   * @param taskId The task ID.
   */
  public void onAck(long taskId) {
    queue.onAck(taskId);
  }

  /**
   * Called when a task is failed.
   *
   * @param taskId The task ID.
   */
  public void onFail(long taskId) {
    queue.onFail(taskId);
  }

}
