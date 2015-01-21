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

import net.kuujo.copycat.state.Initializer;
import net.kuujo.copycat.state.StateContext;

/**
 * Unlocked asynchronous lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnlockedLockState implements LockState {
  private StateContext<LockState> context;

  @Override
  @Initializer
  public void init(StateContext<LockState> context) {
    this.context = context;
  }

  @Override
  public boolean lock(String member, long thread) {
    String currentMember = context.get("member");
    Long currentThread = context.get("thread");
    if ((currentMember != null && !currentMember.equals(member)) || (currentThread != null && !currentThread.equals(thread))) {
      throw new IllegalStateException("Lock is owned by another thread");
    }
    context.put("member", member);
    context.put("thread", thread);
    context.transition(new LockedLockState());
    return true;
  }

  @Override
  public void unlock(String member, long thread) {
    // Do nothing. The lock is already unlocked.
  }

}
