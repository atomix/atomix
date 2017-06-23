/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy;

import io.atomix.event.Event;
import io.atomix.event.EventListener;
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.session.SessionId;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate.
 */
public class RaftProxyDelegate implements RaftProxy {
  private final RaftProxy delegate;

  public RaftProxyDelegate(RaftProxy delegate) {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public String typeName() {
    return delegate.typeName();
  }

  @Override
  public SessionId sessionId() {
    return delegate.sessionId();
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    delegate.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    delegate.removeStateChangeListener(listener);
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftCommand<T> command) {
    return delegate.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(RaftQuery<T> query) {
    return delegate.submit(query);
  }

  @Override
  public <E extends Event> void addEventListener(EventListener<E> listener) {
    delegate.addEventListener(listener);
  }

  @Override
  public <E extends Event> void removeEventListener(EventListener<E> listener) {
    delegate.removeEventListener(listener);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return delegate.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
