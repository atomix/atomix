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

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Initializer;
import net.kuujo.copycat.StateContext;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Asynchronous lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LockState extends Lock {

  /**
   * Initializes the lock state.
   */
  @Initializer
  public void init(StateContext<LockState> context);

  @Override
  @Command
  void lock();

  @Override
  @Command
  void lockInterruptibly() throws InterruptedException;

  @Override
  @Command
  boolean tryLock();

  @Override
  @Command
  boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

  @Override
  @Command
  void unlock();

}
