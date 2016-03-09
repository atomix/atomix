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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTaskQueue {
  protected final MembershipGroup group;
  protected long taskId;
  protected final Map<Long, CompletableFuture<Void>> taskFutures = new ConcurrentHashMap<>();

  protected GroupTaskQueue(MembershipGroup group) {
    this.group = Assert.notNull(group, "group");
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
    submit(taskId, task).whenComplete((result, error) -> {
      if (error != null) {
        taskFutures.remove(taskId);
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Submits the task to the cluster.
   */
  protected CompletableFuture<Void> submit(long taskId, Object task) {
    synchronized (group) {
      Collection<GroupMember> members = group.members();
      CompletableFuture[] futures = new CompletableFuture[members.size()];
      int i = 0;
      for (GroupMember member : members) {
        futures[i++] = group.submit(new GroupCommands.Submit(member.id(), taskId, task));
      }
      return CompletableFuture.allOf(futures);
    }
  }

  /**
   * Handles a task acknowledgement.
   */
  void onAck(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.complete(null);
    }
  }

  /**
   * Handles a task failure.
   */
  void onFail(long taskId) {
    CompletableFuture<Void> future = taskFutures.remove(taskId);
    if (future != null) {
      future.completeExceptionally(new TaskFailedException());
    }
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), group.members());
  }

}
