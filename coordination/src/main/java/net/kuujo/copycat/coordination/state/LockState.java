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

import net.kuujo.copycat.io.log.Compaction;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Lock state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockState extends StateMachine {
  private long version;
  private long time;
  private Commit<LockCommands.Lock> lock;
  private final Queue<Commit<LockCommands.Lock>> queue = new ArrayDeque<>();

  /**
   * Updates the current time.
   */
  private void updateTime(Commit<?> commit) {
    this.time = commit.timestamp();
  }

  /**
   * Applies a lock commit.
   */
  @Apply(LockCommands.Lock.class)
  protected void applyLock(Commit<LockCommands.Lock> commit) {
    updateTime(commit);
    if (lock == null) {
      lock = commit;
      commit.session().publish(true);
      version = commit.index();
    } else if (commit.operation().timeout() == 0) {
      commit.session().publish(false);
      version = commit.index();
    } else {
      queue.add(commit);
    }
  }

  /**
   * Applies an unlock commit.
   */
  @Apply(LockCommands.Unlock.class)
  protected void applyUnlock(Commit<LockCommands.Unlock> commit) {
    updateTime(commit);

    if (lock == null)
      throw new IllegalStateException("not locked");
    if (!lock.session().equals(commit.session()))
      throw new IllegalStateException("not the lock holder");

    lock = queue.poll();
    while (lock != null && (lock.operation().timeout() != -1 && lock.timestamp() + lock.operation().timeout() < time)) {
      version = lock.index();
      lock = queue.poll();
    }

    if (lock != null) {
      lock.session().publish(true);
      version = lock.index();
    }
  }

  /**
   * Filters a lock commit.
   */
  @Filter(LockCommands.Lock.class)
  protected boolean filterLock(Commit<LockCommands.Lock> commit, Compaction compaction) {
    return commit.index() >= version;
  }

  /**
   * Filters an unlock commit.
   */
  @Filter(LockCommands.Lock.class)
  protected boolean filterUnlock(Commit<LockCommands.Unlock> commit, Compaction compaction) {
    return commit.index() >= version;
  }

}
