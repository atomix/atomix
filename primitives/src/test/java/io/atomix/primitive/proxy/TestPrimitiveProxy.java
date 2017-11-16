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
package io.atomix.primitive.proxy;

import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.RaftEvent;
import io.atomix.primitive.operation.RaftOperation;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/**
 * Test primitive proxy.
 */
public class TestPrimitiveProxy implements PrimitiveProxy {
  private final ServiceExecutor executor;

  public TestPrimitiveProxy(ServiceExecutor executor) {
    this.executor = executor;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public SessionId sessionId() {
    return null;
  }

  @Override
  public PrimitiveType serviceType() {
    return null;
  }

  @Override
  public State getState() {
    return null;
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return CompletableFuture.completedFuture(
        executor.apply(new DefaultCommit<>(
            1,
            operation.id(),
            operation.value(),
            mock(Session.class),
            System.currentTimeMillis())));
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {

  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {

  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {

  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {

  }

  @Override
  public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {

  }

  @Override
  public void addEventListener(EventType eventType, Runnable listener) {

  }

  @Override
  public void addEventListener(EventType eventType, Consumer<byte[]> listener) {

  }

  @Override
  public void removeEventListener(EventType eventType, Runnable listener) {

  }

  @Override
  public void removeEventListener(EventType eventType, Consumer listener) {

  }

  @Override
  public CompletableFuture<PrimitiveProxy> open() {
    return null;
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public CompletableFuture<Void> close() {
    return null;
  }

  @Override
  public boolean isClosed() {
    return false;
  }
}
