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
package io.atomix.manager.state;

import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.session.Session;
import io.atomix.resource.InstanceEvent;

import java.util.function.Consumer;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ManagedResourceSession implements ServerSession {
  private final long resource;
  private final ServerSession parent;

  public ManagedResourceSession(long resource, ServerSession parent) {
    this.resource = resource;
    this.parent = parent;
  }

  @Override
  public long id() {
    return parent.id();
  }

  @Override
  public State state() {
    return parent.state();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return parent.onStateChange(callback);
  }

  @Override
  public Session publish(String event) {
    return parent.publish(event, new InstanceEvent<>(resource, null));
  }

  @Override
  public Session publish(String event, Object message) {
    return parent.publish(event, new InstanceEvent<>(resource, message));
  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return parent.equals(object);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), parent.id());
  }

}
