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
 * limitations under the License.
 */
package io.atomix.copycat.coordination;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.coordination.state.MembershipGroupCommands;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Group member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMember {
  private final long memberId;
  private final DistributedMembershipGroup group;

  GroupMember(long memberId, DistributedMembershipGroup group) {
    this.memberId = memberId;
    this.group = Assert.notNull(group, "group");
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public long id() {
    return memberId;
  }

  /**
   * Schedules a callback to run at the given instant.
   *
   * @param instant The instant at which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  public CompletableFuture<Void> schedule(Instant instant, Runnable callback) {
    return schedule(instant.minusMillis(System.currentTimeMillis()), callback);
  }

  /**
   * Schedules a callback to run after the given delay on the member.
   *
   * @param delay The delay after which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  public CompletableFuture<Void> schedule(Duration delay, Runnable callback) {
    return group.submit(MembershipGroupCommands.Schedule.builder()
      .withMember(memberId)
      .withDelay(delay.toMillis())
      .withCallback(callback)
      .build());
  }

  /**
   * Executes a callback on the group member.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has completed.
   */
  public CompletableFuture<Void> execute(Runnable callback) {
    return group.submit(MembershipGroupCommands.Execute.builder()
      .withMember(memberId)
      .withCallback(callback)
      .build());
  }

}
