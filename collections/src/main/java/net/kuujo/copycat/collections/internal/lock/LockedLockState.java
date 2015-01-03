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
import net.kuujo.copycat.cluster.MembershipEvent;

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
    String currentMember = context.get("member");
    if (currentMember == null || context.cluster().member(currentMember) == null) {
      context.transition(new UnlockedLockState());
    } else {
      context.cluster().addMembershipListener(this::handleMembershipEvent);
    }
  }

  /**
   * Handles a cluster membership change event.
   */
  private void handleMembershipEvent(MembershipEvent event) {
    if (event.type() == MembershipEvent.Type.LEAVE) {
      String currentMember = context.get("member");
      if (event.member().uri().equals(currentMember)) {
        context.remove("member");
        context.remove("thread");
        context.transition(new UnlockedLockState());
      }
    }
  }

  @Override
  public boolean lock(String member, long thread) {
    return false;
  }

  @Override
  public void unlock(String member, long thread) {
    String currentMember = context.get("member");
    Long currentThread = context.get("thread");
    if ((currentMember != null && !currentMember.equals(member)) || (currentThread != null && !currentThread.equals(thread))) {
      throw new IllegalStateException("Lock is owned by another thread");
    }
    context.remove("member");
    context.remove("thread");
    context.cluster().removeMembershipListener(this::handleMembershipEvent);
    context.transition(new UnlockedLockState());
  }

}
