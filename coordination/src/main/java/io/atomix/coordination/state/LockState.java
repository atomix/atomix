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
package io.atomix.coordination.state;

import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;

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
public class LockState extends ResourceStateMachine implements SessionListener {
  private Commit<LockCommands.Lock> lock;
  private final Queue<Commit<LockCommands.Lock>> queue = new ArrayDeque<>();
  private final Map<Long, Scheduled> timers = new HashMap<>();

  @Override
  public void register(Session session) {

  }

  @Override
  public void unregister(Session session) {

  }

  @Override
  public void expire(Session session) {

  }

  @Override
  public void close(Session session) {
    if (lock != null && lock.session().id() == session.id()) {
      lock.close();
      lock = queue.poll();
      while (lock != null) {
        Scheduled timer = timers.remove(lock.index());
        if (timer != null)
          timer.cancel();

        if (lock.session().state() == Session.State.CLOSED) {
          lock = queue.poll();
        } else {
          lock.session().publish("lock", true);
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
      commit.session().publish("lock", true);
    } else if (commit.operation().timeout() == 0) {
      try {
        commit.session().publish("lock", false);
      } finally {
        commit.close();
      }
    } else {
      queue.add(commit);
      if (commit.operation().timeout() > 0) {
        timers.put(commit.index(), executor.schedule(Duration.ofMillis(commit.operation().timeout()), () -> {
          timers.remove(commit.index());
          queue.remove(commit);
          commit.close();
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
          throw new IllegalStateException("not the lock holder");

        lock.close();

        lock = queue.poll();
        while (lock != null) {
          Scheduled timer = timers.remove(lock.index());
          if (timer != null)
            timer.cancel();

          if (lock.session().state() == Session.State.CLOSED) {
            lock = queue.poll();
          } else {
            lock.session().publish("lock", true);
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
