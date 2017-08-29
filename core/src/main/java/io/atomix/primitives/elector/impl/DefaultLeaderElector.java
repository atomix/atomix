/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.elector.impl;

import io.atomix.leadership.Leadership;
import io.atomix.leadership.LeadershipEvent;
import io.atomix.cluster.NodeId;
import io.atomix.primitives.PrimitiveException;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.elector.AsyncLeaderElector;
import io.atomix.primitives.elector.LeaderElector;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Default implementation for a {@code LeaderElector} backed by a {@link AsyncLeaderElector}.
 */
public class DefaultLeaderElector extends Synchronous<AsyncLeaderElector> implements LeaderElector {

  private final AsyncLeaderElector asyncElector;
  private final long operationTimeoutMillis;

  public DefaultLeaderElector(AsyncLeaderElector asyncElector, long operationTimeoutMillis) {
    super(asyncElector);
    this.asyncElector = asyncElector;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Leadership run(String topic, NodeId nodeId) {
    return complete(asyncElector.run(topic, nodeId));
  }

  @Override
  public void withdraw(String topic) {
    complete(asyncElector.withdraw(topic));
  }

  @Override
  public boolean anoint(String topic, NodeId nodeId) {
    return complete(asyncElector.anoint(topic, nodeId));
  }

  @Override
  public boolean promote(String topic, NodeId nodeId) {
    return complete(asyncElector.promote(topic, nodeId));
  }

  @Override
  public void evict(NodeId nodeId) {
    complete(asyncElector.evict(nodeId));
  }

  @Override
  public Leadership getLeadership(String topic) {
    return complete(asyncElector.getLeadership(topic));
  }

  @Override
  public Map<String, Leadership> getLeaderships() {
    return complete(asyncElector.getLeaderships());
  }

  @Override
  public void addChangeListener(Consumer<LeadershipEvent> consumer) {
    complete(asyncElector.addChangeListener(consumer));
  }

  @Override
  public void removeChangeListener(Consumer<LeadershipEvent> consumer) {
    complete(asyncElector.removeChangeListener(consumer));
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    asyncElector.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    asyncElector.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return asyncElector.statusChangeListeners();
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }
}
