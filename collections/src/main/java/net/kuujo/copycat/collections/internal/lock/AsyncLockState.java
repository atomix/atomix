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

import net.kuujo.copycat.State;
import net.kuujo.copycat.StateContext;

/**
 * Asynchronous lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncLockState extends State {

  /**
   * Locks the lock.
   *
   * @param context The lock state context.
   */
  void lock(StateContext<AsyncLockState> context);

  /**
   * Unlocks the lock.
   *
   * @param context The lock state context.
   */
  void unlock(StateContext<AsyncLockState> context);

}
