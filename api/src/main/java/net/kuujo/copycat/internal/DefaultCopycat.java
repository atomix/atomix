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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.collections.internal.*;
import net.kuujo.copycat.impl.DefaultStateLog;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final Coordinator coordinator;
  private boolean open;

  public DefaultCopycat(ClusterConfig config, Protocol protocol, ExecutionContext context) {
    this.coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), context);
  }

  @Override
  public Cluster cluster() {
    return coordinator.cluster();
  }

  @Override
  public <T> CompletableFuture<EventLog<T>> eventLog(String name) {
    return eventLog(name, new ClusterConfig().withLocalMember(coordinator.cluster().localMember().uri()));
  }

  @Override
  public <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig config) {
    return coordinator.<EventLog<T>>createResource(name, resource -> new InMemoryLog(), DefaultEventLog::new);
  }

  @Override
  public <T> CompletableFuture<StateLog<T>> stateLog(String name) {
    return stateLog(name, new ClusterConfig().withLocalMember(coordinator.cluster().localMember().uri()));
  }

  @Override
  public <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster) {
    return coordinator.<StateLog<T>>createResource(name, resource -> new InMemoryLog(), DefaultStateLog::new);
  }

  @Override
  public CompletableFuture<StateMachine> stateMachine(String name, StateModel model) {
    return stateMachine(name, model, new ClusterConfig().withLocalMember(coordinator.cluster().localMember().uri()));
  }

  @Override
  public CompletableFuture<StateMachine> stateMachine(String name, StateModel model, ClusterConfig cluster) {
    return null;
  }

  @Override
  public <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name) {
    return coordinator.<AsyncMap<K, V>>createResource(name, resource -> new InMemoryLog(), DefaultAsyncMap::new);
  }

  @Override
  public <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name) {
    return coordinator.<AsyncMultiMap<K, V>>createResource(name, resource -> new InMemoryLog(), DefaultAsyncMultiMap::new);
  }

  @Override
  public <T> CompletableFuture<AsyncList<T>> getList(String name) {
    return coordinator.<AsyncList<T>>createResource(name, resource -> new InMemoryLog(), DefaultAsyncList::new);
  }

  @Override
  public <T> CompletableFuture<AsyncSet<T>> getSet(String name) {
    return coordinator.<AsyncSet<T>>createResource(name, resource -> new InMemoryLog(), DefaultAsyncSet::new);
  }

  @Override
  public CompletableFuture<AsyncLock> getLock(String name) {
    return coordinator.<AsyncLock>createResource(name, resource -> new InMemoryLog(), DefaultAsyncLock::new);
  }

  @Override
  public CompletableFuture<Void> open() {
    return coordinator.open();
  }

  @Override
  public CompletableFuture<Void> close() {
    return coordinator.close();
  }

}
