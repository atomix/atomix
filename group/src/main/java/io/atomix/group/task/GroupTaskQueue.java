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

import io.atomix.group.GroupMember;
import io.atomix.group.MembershipGroup;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Group task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTaskQueue extends TaskQueue {

  public GroupTaskQueue(MembershipGroup group) {
    super(group);
  }

  @Override
  public CompletableFuture<Void> submit(Object task) {
    Collection<GroupMember> members = members();
    CompletableFuture[] futures = new CompletableFuture[members.size()];
    int i = 0;
    for (GroupMember member : members) {
      futures[i++] = member.tasks().submit(task);
    }
    return CompletableFuture.allOf(futures);
  }

}
