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
package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.state.GroupCommands;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTaskQueue {
  private final String memberId;
  private final MembershipGroup group;
  private long taskId;
  private final Map<Long, CompletableFuture<Void>> taskFutures = new ConcurrentHashMap<>();

  protected GroupTaskQueue(MembershipGroup group, String memberId) {
    this.group = Assert.notNull(group, "group");
    this.memberId = memberId;
  }

  /**
   * Submits a task.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been acknowledged.
   */
  public CompletableFuture<Void> submit(Object task) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final long taskId = ++this.taskId;
    taskFutures.put(taskId, future);
    group.submit(new GroupCommands.Submit(memberId, taskId, task)).whenComplete((result, error) -> {
      if (error != null) {
        taskFutures.remove(taskId);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Handles a task acknowledgement.
   */
  void handleAck(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.complete(null);
    }
  }

  /**
   * Handles a task failure.
   */
  void handleFail(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.completeExceptionally(new TaskFailedException());
    }
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
