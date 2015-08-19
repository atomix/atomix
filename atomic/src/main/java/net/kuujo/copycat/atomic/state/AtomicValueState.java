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

import net.kuujo.copycat.PersistenceMode;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Atomic reference state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AtomicValueState extends StateMachine {
  private final Set<Long> sessions = new HashSet<>();
  private final Map<Session, Commit<AtomicValueCommands.Listen>> listeners = new HashMap<>();
  private final AtomicReference<Object> value = new AtomicReference<>();
  private Commit<? extends AtomicValueCommands.ReferenceCommand> current;

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(AtomicValueCommands.Listen.class, this::listen);
    executor.register(AtomicValueCommands.Unlisten.class, this::unlisten);
    executor.register(AtomicValueCommands.Get.class, (Function<Commit<AtomicValueCommands.Get>, Object>) this::get);
    executor.register(AtomicValueCommands.Set.class, this::set);
    executor.register(AtomicValueCommands.CompareAndSet.class, this::compareAndSet);
    executor.register(AtomicValueCommands.GetAndSet.class, (Function<Commit< AtomicValueCommands.GetAndSet>, Object>) this::getAndSet);
  }

  @Override
  public void register(Session session) {
    sessions.add(session.id());
  }

  @Override
  public void expire(Session session) {
    sessions.remove(session.id());
    Commit<AtomicValueCommands.Listen> listener = listeners.remove(session);
    if (listener != null) {
      listener.clean();
    }
  }

  @Override
  public void close(Session session) {
    sessions.remove(session.id());
    Commit<AtomicValueCommands.Listen> listener = listeners.remove(session);
    if (listener != null) {
      listener.clean();
    }
  }

  /**
   * Returns a boolean value indicating whether the given commit is active.
   */
  private boolean isActive(Commit<? extends AtomicValueCommands.ReferenceCommand> commit, Instant time) {
    if (commit == null) {
      return false;
    } else if (commit.operation().mode() == PersistenceMode.EPHEMERAL && !sessions.contains(commit.session().id())) {
      return false;
    } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time.toEpochMilli() - commit.time().toEpochMilli()) {
      return false;
    }
    return true;
  }

  /**
   * Handles a listen commit.
   */
  protected void listen(Commit<AtomicValueCommands.Listen> commit) {
    if (!commit.session().isOpen()) {
      commit.clean();
    } else {
      listeners.put(commit.session(), commit);
    }
  }

  /**
   * Handles an unlisten commit.
   */
  protected void unlisten(Commit<AtomicValueCommands.Unlisten> commit) {
    Commit<AtomicValueCommands.Listen> listener = listeners.remove(commit.session());
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
  protected Object get(Commit<AtomicValueCommands.Get> commit) {
    try {
      return current != null && isActive(current, commit.time()) ? value.get() : null;
    } finally {
      commit.close();
    }
  }

  /**
   * Applies a set commit.
   */
  protected void set(Commit<AtomicValueCommands.Set> commit) {
    if (!isActive(commit, context().time().instant())) {
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
  protected boolean compareAndSet(Commit<AtomicValueCommands.CompareAndSet> commit) {
    if (!isActive(commit, context().time().instant())) {
      commit.clean();
      return false;
    } else if (isActive(current, commit.time())) {
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
  protected Object getAndSet(Commit<AtomicValueCommands.GetAndSet> commit) {
    if (!isActive(commit, context().time().instant())) {
      commit.clean();

    }

    if (isActive(current, commit.time())) {
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
