/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy delegate.
 */
public class DelegatingRaftProxyClient implements RaftProxyClient {
  private final RaftProxyClient delegate;

  public DelegatingRaftProxyClient(RaftProxyClient delegate) {
    this.delegate = checkNotNull(delegate, "delegate cannot be null");
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public ServiceType serviceType() {
    return delegate.serviceType();
  }

  @Override
  public SessionId sessionId() {
    return delegate.sessionId();
  }

  @Override
  public ServiceRevision revision() {
    return delegate.revision();
  }

  @Override
  public RaftProxy.State getState() {
    return delegate.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
    delegate.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
    delegate.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return delegate.execute(operation);
  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {
    delegate.addEventListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {
    delegate.removeEventListener(listener);
  }

  @Override
  public CompletableFuture<RaftProxyClient> open() {
    return delegate.open();
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
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
