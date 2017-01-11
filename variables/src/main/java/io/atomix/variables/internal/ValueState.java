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
package io.atomix.variables.internal;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.copycat.server.Commit;
import io.atomix.variables.DistributedValue;

import java.time.Duration;
import java.util.Properties;

/**
 * Distributed value state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ValueState<T> extends AbstractValueState<T> {
  protected T value;
  protected Commit<? extends ValueCommands.ValueCommand<?>> current;
  protected Scheduled timer;

  public ValueState(Properties config) {
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
    T oldValue = value;
    value = commit.operation().value();
    setCurrent(commit);
    notify(new DistributedValue.ChangeEvent<>(oldValue, value));
  }

  /**
   * Handles a compare and set commit.
   */
  public boolean compareAndSet(Commit<ValueCommands.CompareAndSet<T>> commit) {
    if ((value == null && commit.operation().expect() == null) || (value != null && commit.operation().expect() != null && value.equals(commit.operation().expect()))) {
      T oldValue = value;
      value = commit.operation().update();
      cleanCurrent();
      setCurrent(commit);
      notify(new DistributedValue.ChangeEvent<>(oldValue, value));
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
    T oldValue = value;
    value = commit.operation().value();
    cleanCurrent();
    setCurrent(commit);
    notify(new DistributedValue.ChangeEvent<>(oldValue, value));
    return oldValue;
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
