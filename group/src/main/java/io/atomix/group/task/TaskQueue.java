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
package io.atomix.group.task;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.GroupMember;
import io.atomix.group.MembershipGroup;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class TaskQueue {
  protected final MembershipGroup group;

  protected TaskQueue(MembershipGroup group) {
    this.group = Assert.notNull(group, "group");
  }

  /**
   * Submits a task.
   *
   * @param task The task to submit.
   * @return A completable future to be completed once the task has been acknowledged.
   */
  public abstract CompletableFuture<Void> submit(Object task);

  /**
   * Returns the collection of group members to which to send tasks.
   */
  protected Collection<GroupMember> members() {
    return group.members();
  }

  /**
   * Handles a task acknowledgement.
   */
  void onAck(long taskId) {
  }

  /**
   * Handles a task failure.
   */
  void onFail(long taskId) {
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), group.members());
  }

}
