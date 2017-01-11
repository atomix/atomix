/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.variables.internal;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Abstract distributed value state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AbstractValueState<T> extends ResourceStateMachine {
  protected final Set<ServerSession> listeners = new HashSet<>();
  protected T value;
  protected Commit<? extends ValueCommands.ValueCommand<?>> current;
  protected Scheduled timer;

  public AbstractValueState(Properties config) {
    super(config);
  }

  /**
   * Handles a get commit.
   */
  public T get(Commit<ValueCommands.Get<T>> commit) {
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
      current.close();
    }
  }

  /**
   * Sets the current commit.
   */
  private void setCurrent(Commit<? extends ValueCommands.ValueCommand<?>> commit) {
    timer = commit.operation().ttl() > 0 ? executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
      value = null;
      current.close();
      current = null;
    }) : null;
    current = commit;
  }

  /**
   * Applies a set commit.
   */
  public void set(Commit<ValueCommands.Set<T>> commit) {
    cleanCurrent();
    value = commit.operation().value();
    setCurrent(commit);
  }

  /**
   * Handles a compare and set commit.
   */
  public boolean compareAndSet(Commit<ValueCommands.CompareAndSet<T>> commit) {
    if ((value == null && commit.operation().expect() == null) || (value != null && commit.operation().expect() != null && value.equals(commit.operation().expect()))) {
      value = commit.operation().update();
      cleanCurrent();
      setCurrent(commit);
      return true;
    } else {
      commit.close();
      return false;
    }
  }

  /**
   * Handles a get and set commit.
   */
  public T getAndSet(Commit<ValueCommands.GetAndSet<T>> commit) {
    T result = value;
    value = commit.operation().value();
    cleanCurrent();
    setCurrent(commit);
    return result;
  }

  @Override
  public void delete() {
    if (current != null) {
      current.close();
      current = null;
      value = null;
    }
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }

}
