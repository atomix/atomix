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
package io.atomix.copycat.coordination.state;

import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;
import io.atomix.catalyst.util.concurrent.Scheduled;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Lock state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockState extends StateMachine {
  private Commit<LockCommands.Lock> lock;
  private final Queue<Commit<LockCommands.Lock>> queue = new ArrayDeque<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

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
      commit.session().publish("lock", true);
    } else if (commit.operation().timeout() == 0) {
      commit.session().publish("lock", false);
    } else {
      queue.add(commit);
      if (commit.operation().timeout() > 0) {
        timers.put(commit.index(), executor().schedule(Duration.ofMillis(commit.operation().timeout()), () -> {
          timers.remove(commit.index());
          queue.remove(commit);
          commit.clean();
        }));
      }
    }
  }

  /**
   * Applies an unlock commit.
   */
  protected void unlock(Commit<LockCommands.Unlock> commit) {
    try {
      if (lock != null) {
        if (!lock.session().equals(commit.session()))
          throw new IllegalStateException("not the lock holder");

        lock.clean();

        lock = queue.poll();
        if (lock != null) {
          Scheduled timer = timers.remove(lock.index());
          if (timer != null)
            timer.cancel();
          lock.session().publish("lock", true);
        }
      }
    } finally {
      commit.clean();
    }
  }

}
