/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;

import java.util.concurrent.CompletableFuture;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ClusterCoordinator coordinator;
  private final CopycatConfig config;

  public DefaultCopycat(String uri, CopycatConfig config) {
    this.coordinator = new DefaultClusterCoordinator(uri, config.resolve());
    this.config = config;
  }

  @Override
  public Cluster cluster() {
    return coordinator.cluster();
  }

  @Override
  public CopycatConfig config() {
    return config;
  }

  @Override
  public <T, U> EventLog<T, U> eventLog(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public <T, U> StateLog<T, U> stateLog(String name) {
    return null;
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState) {
    return coordinator.getResource(name);
  }

  @Override
  public LeaderElection leaderElection(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public <K, V> AsyncMap<K, V> map(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> multiMap(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public <T> AsyncList<T> list(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public <T> AsyncSet<T> set(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public AsyncLock lock(String name) {
    return coordinator.getResource(name);
  }

  @Override
  public synchronized CompletableFuture<Copycat> open() {
    return coordinator.open().thenApply(v -> this);
  }

  @Override
  public synchronized boolean isOpen() {
    return coordinator.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return coordinator.close();
  }

  @Override
  public synchronized boolean isClosed() {
    return coordinator.isClosed();
  }

}
