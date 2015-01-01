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

import net.kuujo.copycat.StateContext;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Locked asynchronous lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockedLockState implements LockState {
  private StateContext<LockState> context;

  @Override
  public void init(StateContext<LockState> context) {
    this.context = context;
  }

  @Override
  public void lock() {
    throw new IllegalStateException("Lock is locked");
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    throw new UnsupportedOperationException("lockInterruptibly");
  }

  @Override
  public boolean tryLock() {
    return false;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public void unlock() {
    context.transition(new UnlockedLockState());
  }

  @NotNull
  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException("newCondition");
  }

}
