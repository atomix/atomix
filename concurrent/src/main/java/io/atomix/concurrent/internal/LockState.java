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
package io.atomix.concurrent.internal;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.*;

/**
 * Lock state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockState extends ResourceStateMachine implements SessionListener {
  private Commit<LockCommands.Lock> lock;
  private final Queue<Commit<LockCommands.Lock>> queue = new ArrayDeque<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  public LockState(Properties config) {
    super(config);
  }

  @Override
  public void close(ServerSession session) {
    if (lock != null && lock.session().id() == session.id()) {
      lock.close();
      lock = queue.poll();
      while (lock != null) {
        Scheduled timer = timers.remove(lock.index());
        if (timer != null)
          timer.cancel();

        if (lock.session().state() == ServerSession.State.EXPIRED || lock.session().state() == ServerSession.State.CLOSED) {
          lock = queue.poll();
        } else {
          lock.session().publish("lock", new LockCommands.LockEvent(lock.operation().id(), lock.index()));
          break;
        }
      }
    }
  }

  /**
   * Applies a lock commit.
   */
  public void lock(Commit<LockCommands.Lock> commit) {
    if (lock == null) {
      lock = commit;
      commit.session().publish("lock", new LockCommands.LockEvent(commit.operation().id(), commit.index()));
    } else if (commit.operation().timeout() == 0) {
      try {
        commit.session().publish("fail", new LockCommands.LockEvent(commit.operation().id(), commit.index()));
      } finally {
        commit.close();
      }
    } else {
      queue.add(commit);
      if (commit.operation().timeout() > 0) {
        timers.put(commit.index(), executor.schedule(Duration.ofMillis(commit.operation().timeout()), () -> {
          try {
            timers.remove(commit.index());
            queue.remove(commit);
            if (commit.session().state().active()) {
              commit.session().publish("fail", new LockCommands.LockEvent(commit.operation().id(), commit.index()));
            }
          } finally {
            commit.close();
          }
        }));
      }
    }
  }

  /**
   * Applies an unlock commit.
   */
  public void unlock(Commit<LockCommands.Unlock> commit) {
    try {
      if (lock != null) {
        if (!lock.session().equals(commit.session()))
          return;

        lock.close();

        lock = queue.poll();
        while (lock != null) {
          Scheduled timer = timers.remove(lock.index());
          if (timer != null)
            timer.cancel();

          if (lock.session().state() == ServerSession.State.EXPIRED || lock.session().state() == ServerSession.State.CLOSED) {
            lock = queue.poll();
          } else {
            lock.session().publish("lock", new LockCommands.LockEvent(lock.operation().id(), lock.index()));
            break;
          }
        }
      }
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    if (lock != null) {
      lock.close();
    }

    queue.forEach(Commit::close);
    queue.clear();

    timers.values().forEach(Scheduled::cancel);
    timers.clear();
  }

}
