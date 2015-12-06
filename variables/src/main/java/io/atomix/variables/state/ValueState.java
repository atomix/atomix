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
package io.atomix.variables.state;

import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Distributed value state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ValueState extends ResourceStateMachine {
  private final Map<Session, Commit<ValueCommands.Listen>> listeners = new HashMap<>();
  private Object value;
  private Commit<? extends ValueCommands.ValueCommand> current;
  private Scheduled timer;

  /**
   * Handles a listen commit.
   */
  public void listen(Commit<ValueCommands.Listen> commit) {
    listeners.put(commit.session(), commit);
    commit.session().onClose(s -> {
      Commit<ValueCommands.Listen> listener = listeners.remove(commit.session());
      if (listener != null) {
        listener.clean();
      }
    });
  }

  /**
   * Handles an unlisten commit.
   */
  public void unlisten(Commit<ValueCommands.Unlisten> commit) {
    try {
      Commit<ValueCommands.Listen> listener = listeners.remove(commit.session());
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
  public Object get(Commit<ValueCommands.Get> commit) {
    try {
      return current != null ? value : null;
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
  private void setCurrent(Commit<? extends ValueCommands.ValueCommand> commit) {
    timer = commit.operation().ttl() > 0 ? executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
      value = null;
      current.clean();
      current = null;
    }) : null;
    current = commit;
    change(value);
  }

  /**
   * Applies a set commit.
   */
  public void set(Commit<ValueCommands.Set> commit) {
    cleanCurrent();
    value = commit.operation().value();
    setCurrent(commit);
  }

  /**
   * Handles a compare and set commit.
   */
  public boolean compareAndSet(Commit<ValueCommands.CompareAndSet> commit) {
    if ((value == null && commit.operation().expect() == null) || (value != null && commit.operation().expect() != null && value.equals(commit.operation().expect()))) {
      value = commit.operation().update();
      cleanCurrent();
      setCurrent(commit);
      return true;
    } else {
      commit.clean();
      return false;
    }
  }

  /**
   * Handles a get and set commit.
   */
  public Object getAndSet(Commit<ValueCommands.GetAndSet> commit) {
    Object result = value;
    value = commit.operation().value();
    cleanCurrent();
    setCurrent(commit);
    return result;
  }

  @Override
  public void delete() {
    if (current != null) {
      current.clean();
      current = null;
      value = null;
    }
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }

}
