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
package net.kuujo.copycat.example.lockservice;

import net.kuujo.copycat.state.Initializer;
import net.kuujo.copycat.state.StateContext;

/**
 * Locked lock state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockedLockState implements LockState {
  private StateContext<LockState> context;

  @Initializer
  public void init(StateContext<LockState> context) {
    this.context = context;
  }

  @Override
  public void lock(String holder) {
    throw new IllegalStateException("Lock is already locked by " + context.get("holder"));
  }

  @Override
  public void unlock(String holder) {
    context.remove("holder");
    context.transition(new UnlockedLockState());
  }

}
