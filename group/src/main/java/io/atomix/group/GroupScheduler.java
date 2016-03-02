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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Group scheduler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupScheduler {
  private final String memberId;
  private final DistributedGroup group;

  protected GroupScheduler(String memberId, DistributedGroup group) {
    this.memberId = memberId;
    this.group = Assert.notNull(group, "group");
  }

  /**
   * Schedules a callback to be run after the given delay.
   *
   * @param delay The delay after which to run the callback.
   * @param callback The callback to run after the given delay.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  public CompletableFuture<Void> schedule(Duration delay, Runnable callback) {
    return group.submit(new GroupCommands.Schedule(memberId, delay.toMillis(), callback));
  }

  /**
   * Executes a remote callback.
   *
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  public CompletableFuture<Void> execute(Runnable callback) {
    return group.submit(new GroupCommands.Execute(memberId, callback));
  }

  @Override
  public String toString() {
    if (memberId == null) {
      return getClass().getSimpleName();
    }
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
