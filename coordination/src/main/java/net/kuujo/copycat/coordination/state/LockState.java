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
package net.kuujo.copycat.coordination.state;

import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Lock state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockState extends StateMachine {
  private Commit<LockCommands.Lock> lock;
  private final Queue<Commit<LockCommands.Lock>> queue = new ArrayDeque<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(LockCommands.Lock.class, this::lock);
    executor.register(LockCommands.Unlock.class, this::unlock);
  }

  /**
   * Applies a lock commit.
   */
  protected void lock(Commit<LockCommands.Lock> commit) {
    if (lock == null) {
      lock = commit;
      commit.session().publish(true);
    } else if (commit.operation().timeout() == 0) {
      commit.session().publish(false);
    } else {
      queue.add(commit);
    }
  }

  /**
   * Applies an unlock commit.
   */
  protected void unlock(Commit<LockCommands.Unlock> commit) {
    if (lock == null) {
      commit.clean();
    } else {
      lock.clean();

      if (!lock.session().equals(commit.session()))
        throw new IllegalStateException("not the lock holder");

      lock = queue.poll();
      while (lock != null && (lock.operation().timeout() != -1 && lock.time().toEpochMilli() + lock.operation().timeout() < now().toEpochMilli())) {
        lock.clean();
        lock = queue.poll();
      }

      if (lock != null) {
        lock.session().publish(true);
      }
    }
  }

}
