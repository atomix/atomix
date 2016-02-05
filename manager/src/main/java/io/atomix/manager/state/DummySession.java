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
package io.atomix.manager.state;

import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.client.session.Session;

import java.util.function.Consumer;

/**
 * Dummy session.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class DummySession implements Session {
  private final long id;

  public DummySession(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public State state() {
    return State.CLOSED;
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return new DummyListener<>();
  }

  @Override
  public Session publish(String event) {
    return this;
  }

  @Override
  public Session publish(String event, Object message) {
    return this;
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return new DummyListener<>();
  }

  @Override
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    return new DummyListener<>();
  }

  /**
   * Dummy listener.
   */
  private static class DummyListener<T> implements Listener<T> {
    @Override
    public void accept(T t) {
    }
    @Override
    public void close() {
    }
  }

}
