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
package io.atomix.coordination;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Provides an interface to interacting with members of a {@link DistributedMembershipGroup}.
 * <p>
 * A {@code GroupMember} represents a reference to a single instance of a resource which has
 * {@link DistributedMembershipGroup#join() joined} a membership group. Each member is guaranteed to
 * have a unique {@link #id()} throughout the lifetime of the distributed resource. Group members
 * can {@link #schedule(Duration, Runnable) schedule} or {@link #execute(Runnable) execute} callbacks
 * remotely on member nodes.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface GroupMember {

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  long id();

  /**
   * Schedules a callback to run at the given instant.
   *
   * @param instant The instant at which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  CompletableFuture<Void> schedule(Instant instant, Runnable callback);

  /**
   * Schedules a callback to run after the given delay on the member.
   *
   * @param delay The delay after which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  CompletableFuture<Void> schedule(Duration delay, Runnable callback);

  /**
   * Executes a callback on the group member.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has completed.
   */
  CompletableFuture<Void> execute(Runnable callback);

}
