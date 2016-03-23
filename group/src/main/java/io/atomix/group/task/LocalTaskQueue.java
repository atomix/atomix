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

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.group.MembershipGroup;
import io.atomix.group.util.Submitter;

import java.util.function.Consumer;

/**
 * Local member task queue.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LocalTaskQueue extends MemberTaskQueue {
  private final Listeners<Task<Object>> taskListeners = new Listeners<>();

  public LocalTaskQueue(String memberId, MembershipGroup group, Submitter submitter) {
    super(memberId, group, submitter);
  }

  /**
   * Registers a consumer for tasks send to the local member.
   *
   * @param consumer The task consumer.
   * @param <T> The task type.
   * @return The task listener.
   */
  @SuppressWarnings("unchecked")
  public <T> Listener<Task<T>> onTask(Consumer<Task<T>> consumer) {
    return (Listener) taskListeners.add((Consumer) consumer);
  }

  /**
   * Handles a task.
   */
  @SuppressWarnings("unchecked")
  void onTask(Task task) {
    taskListeners.accept(task);
  }

}
