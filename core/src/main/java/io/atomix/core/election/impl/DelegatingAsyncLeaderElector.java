/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.election.impl;

import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating asynchronous leader elector.
 */
public class DelegatingAsyncLeaderElector<T> extends DelegatingAsyncPrimitive<AsyncLeaderElector<T>> implements AsyncLeaderElector<T> {
  public DelegatingAsyncLeaderElector(AsyncLeaderElector<T> primitive) {
    super(primitive);
  }

  @Override
  public CompletableFuture<Leadership<T>> run(String topic, T identifier) {
    return delegate().run(topic, identifier);
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, T identifier) {
    return delegate().withdraw(topic, identifier);
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, T identifier) {
    return delegate().anoint(topic, identifier);
  }

  @Override
  public CompletableFuture<Void> evict(T identifier) {
    return delegate().evict(identifier);
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, T identifier) {
    return delegate().promote(topic, identifier);
  }

  @Override
  public CompletableFuture<Leadership<T>> getLeadership(String topic) {
    return delegate().getLeadership(topic);
  }

  @Override
  public CompletableFuture<Map<String, Leadership<T>>> getLeaderships() {
    return delegate().getLeaderships();
  }

  @Override
  public CompletableFuture<Void> addListener(LeadershipEventListener<T> listener) {
    return delegate().addListener(listener);
  }

  @Override
  public CompletableFuture<Void> removeListener(LeadershipEventListener<T> listener) {
    return delegate().removeListener(listener);
  }

  @Override
  public CompletableFuture<Void> addListener(String topic, LeadershipEventListener<T> listener) {
    return delegate().addListener(topic, listener);
  }

  @Override
  public CompletableFuture<Void> removeListener(String topic, LeadershipEventListener<T> listener) {
    return delegate().removeListener(topic, listener);
  }

  @Override
  public LeaderElector<T> sync(Duration operationTimeout) {
    return new BlockingLeaderElector<>(this, operationTimeout.toMillis());
  }
}
