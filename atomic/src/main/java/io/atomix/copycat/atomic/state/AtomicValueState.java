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
package io.atomix.copycat.atomic.state;

import io.atomix.catalog.client.session.Session;
import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;
import io.atomix.catalyst.util.concurrent.Scheduled;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Atomic reference state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AtomicValueState extends StateMachine {
  private final Map<Session, Commit<AtomicValueCommands.Listen>> listeners = new HashMap<>();
  private final AtomicReference<Object> value = new AtomicReference<>();
  private Commit<? extends AtomicValueCommands.ValueCommand> current;
  private Scheduled timer;

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(AtomicValueCommands.Listen.class, this::listen);
    executor.register(AtomicValueCommands.Unlisten.class, this::unlisten);
    executor.register(AtomicValueCommands.Get.class, (Function<Commit<AtomicValueCommands.Get>, Object>) this::get);
    executor.register(AtomicValueCommands.Set.class, this::set);
    executor.register(AtomicValueCommands.CompareAndSet.class, this::compareAndSet);
    executor.register(AtomicValueCommands.GetAndSet.class, (Function<Commit<AtomicValueCommands.GetAndSet>, Object>) this::getAndSet);
  }

  /**
   * Handles a listen commit.
   */
  protected void listen(Commit<AtomicValueCommands.Listen> commit) {
    listeners.put(commit.session(), commit);
    commit.session().onClose(s -> {
      Commit<AtomicValueCommands.Listen> listener = listeners.remove(commit.session());
      if (listener != null) {
        listener.clean();
      }
    });
  }

  /**
   * Handles an unlisten commit.
   */
  protected void unlisten(Commit<AtomicValueCommands.Unlisten> commit) {
    try {
      Commit<AtomicValueCommands.Listen> listener = listeners.remove(commit.session());
      if (listener != null) {
        listener.clean();
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Triggers a change event.
   */
  private void change(Object value) {
    for (Session session : listeners.keySet()) {
      session.publish("change", value);
    }
  }

  /**
   * Handles a get commit.
   */
  protected Object get(Commit<AtomicValueCommands.Get> commit) {
    try {
      return current != null ? value.get() : null;
    } finally {
      commit.close();
    }
  }

  /**
   * Cleans the current commit.
   */
  private void cleanCurrent() {
    if (current != null) {
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
      current.clean();
    }
  }

  /**
   * Sets the current commit.
   */
  private void setCurrent(Commit<? extends AtomicValueCommands.ValueCommand> commit) {
    timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
      value.set(null);
      current.clean();
      current = null;
    }) : null;
    current = commit;
    change(value.get());
  }

  /**
   * Applies a set commit.
   */
  protected void set(Commit<AtomicValueCommands.Set> commit) {
    cleanCurrent();
    value.set(commit.operation().value());
    setCurrent(commit);
  }

  /**
   * Handles a compare and set commit.
   */
  protected boolean compareAndSet(Commit<AtomicValueCommands.CompareAndSet> commit) {
    if (value.compareAndSet(commit.operation().expect(), commit.operation().update())) {
      cleanCurrent();
      setCurrent(commit);
      return true;
    }
    return false;
  }

  /**
   * Handles a get and set commit.
   */
  protected Object getAndSet(Commit<AtomicValueCommands.GetAndSet> commit) {
    Object result = value.getAndSet(commit.operation().value());
    cleanCurrent();
    setCurrent(commit);
    return result;
  }

}
