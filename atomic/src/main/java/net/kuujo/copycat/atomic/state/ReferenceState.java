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
package net.kuujo.copycat.atomic.state;

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Atomic reference state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReferenceState extends StateMachine {
  private final Set<Long> sessions = new HashSet<>();
  private final Map<Session, Commit<ReferenceCommands.Listen>> listeners = new HashMap<>();
  private final AtomicReference<Object> value = new AtomicReference<>();
  private Commit<? extends ReferenceCommands.ReferenceCommand> current;

  @Override
  public void register(Session session) {
    sessions.add(session.id());
  }

  @Override
  public void expire(Session session) {
    sessions.remove(session.id());
    Commit<ReferenceCommands.Listen> listener = listeners.remove(session);
    if (listener != null) {
      listener.clean();
    }
  }

  @Override
  public void close(Session session) {
    sessions.remove(session.id());
    Commit<ReferenceCommands.Listen> listener = listeners.remove(session);
    if (listener != null) {
      listener.clean();
    }
  }

  /**
   * Returns a boolean value indicating whether the given commit is active.
   */
  private boolean isActive(Commit<? extends ReferenceCommands.ReferenceCommand> commit, long time) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceLevel.EPHEMERAL && !sessions.contains(commit.session().id())) {
      return false;
    } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time - commit.timestamp()) {
      return false;
    }
    return true;
  }

  /**
   * Handles a listen commit.
   */
  @Apply(ReferenceCommands.Listen.class)
  protected void listen(Commit<ReferenceCommands.Listen> commit) {
    if (!commit.session().isOpen()) {
      commit.clean();
    } else {
      listeners.put(commit.session(), commit);
    }
  }

  /**
   * Handles an unlisten commit.
   */
  @Apply(ReferenceCommands.Unlisten.class)
  protected void unlisten(Commit<ReferenceCommands.Unlisten> commit) {
    Commit<ReferenceCommands.Listen> listener = listeners.remove(commit.session());
    if (listener == null) {
      commit.clean();
    }
  }

  /**
   * Triggers a change event.
   */
  private void change(Object value) {
    for (Session session : listeners.keySet()) {
      session.publish(value);
    }
  }

  /**
   * Handles a get commit.
   */
  @Apply(ReferenceCommands.Get.class)
  protected Object get(Commit<ReferenceCommands.Get> commit) {
    try {
      return current != null && isActive(current, commit.timestamp()) ? value.get() : null;
    } finally {
      commit.close();
    }
  }

  /**
   * Applies a set commit.
   */
  @Apply(ReferenceCommands.Set.class)
  protected void set(Commit<ReferenceCommands.Set> commit) {
    if (!isActive(commit, getTime())) {
      commit.clean();
    } else {
      if (current != null) {
        current.clean();
      }
      value.set(commit.operation().value());
      current = commit;
      change(value.get());
    }
  }

  /**
   * Handles a compare and set commit.
   */
  @Apply(ReferenceCommands.CompareAndSet.class)
  protected boolean compareAndSet(Commit<ReferenceCommands.CompareAndSet> commit) {
    if (!isActive(commit, getTime())) {
      commit.clean();
      return false;
    } else if (isActive(current, commit.timestamp())) {
      if (value.compareAndSet(commit.operation().expect(), commit.operation().update())) {
        if (current != null) {
          current.clean();
        }
        current = commit;
        change(value.get());
        return true;
      }
      return false;
    } else if (commit.operation().expect() == null) {
      if (current != null) {
        current.clean();
      }
      value.set(null);
      current = commit;
      change(null);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handles a get and set commit.
   */
  @Apply(ReferenceCommands.GetAndSet.class)
  protected Object getAndSet(Commit<ReferenceCommands.GetAndSet> commit) {
    if (!isActive(commit, getTime())) {
      commit.clean();

    }

    if (isActive(current, commit.timestamp())) {
      if (current != null) {
        current.clean();
      }
      Object result = value.getAndSet(commit.operation().value());
      current = commit;
      change(value.get());
      return result;
    } else {
      if (current != null) {
        current.clean();
      }
      value.set(commit.operation().value());
      current = commit;
      change(value.get());
      return null;
    }
  }

}
