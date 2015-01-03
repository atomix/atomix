/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections.internal.lock;

import net.kuujo.copycat.Initializer;
import net.kuujo.copycat.StateContext;

/**
 * Asynchronous lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LockState {

  /**
   * Initializes the lock state.
   */
  @Initializer
  public void init(StateContext<LockState> context);

  /**
   * Locks the lock.
   *
   * @param member The member that is unlocking the lock.
   * @param thread The thread that is unlocking the lock.
   * @return Indicates whether the lock was successfully locked.
   */
  boolean lock(String member, long thread);

  /**
   * Unlocks the lock.
   *
   * @param member The member that is unlocking the lock.
   * @param thread The thread that is unlocking the lock.
   */
  void unlock(String member, long thread);

}
