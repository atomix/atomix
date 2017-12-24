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
package io.atomix.core.election.impl;

import com.google.common.collect.Maps;

import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link AsyncLeaderElector} that has its topics partitioned horizontally across
 * several {@link AsyncLeaderElector leader electors}.
 */
public class PartitionedAsyncLeaderElector<T> implements AsyncLeaderElector<T> {

  private final String name;
  private final TreeMap<PartitionId, AsyncLeaderElector<T>> partitions = Maps.newTreeMap();
  private final Partitioner<String> topicHasher;

  public PartitionedAsyncLeaderElector(
      String name,
      Map<PartitionId, AsyncLeaderElector<T>> partitions,
      Partitioner<String> topicHasher) {
    this.name = name;
    this.partitions.putAll(checkNotNull(partitions));
    this.topicHasher = checkNotNull(topicHasher);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletableFuture<Leadership<T>> run(String topic, T id) {
    return getLeaderElector(topic).run(topic, id);
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, T id) {
    return getLeaderElector(topic).withdraw(topic, id);
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, T id) {
    return getLeaderElector(topic).anoint(topic, id);
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, T id) {
    return getLeaderElector(topic).promote(topic, id);
  }

  @Override
  public CompletableFuture<Void> evict(T id) {
    return CompletableFuture.allOf(getLeaderElectors().stream()
        .map(elector -> elector.evict(id))
        .toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<Leadership<T>> getLeadership(String topic) {
    return getLeaderElector(topic).getLeadership(topic);
  }

  @Override
  public CompletableFuture<Map<String, Leadership<T>>> getLeaderships() {
    Map<String, Leadership<T>> leaderships = Maps.newConcurrentMap();
    return CompletableFuture.allOf(getLeaderElectors().stream()
        .map(elector -> elector.getLeaderships()
            .thenAccept(m -> leaderships.putAll(m)))
        .toArray(CompletableFuture[]::new))
        .thenApply(v -> leaderships);
  }

  @Override
  public CompletableFuture<Void> addListener(LeadershipEventListener<T> listener) {
    return CompletableFuture.allOf(getLeaderElectors().stream()
        .map(map -> map.addListener(listener))
        .toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<Void> removeListener(LeadershipEventListener<T> listener) {
    return CompletableFuture.allOf(getLeaderElectors().stream()
        .map(map -> map.removeListener(listener))
        .toArray(CompletableFuture[]::new));
  }

  /**
   * Returns the leaderElector (partition) to which the specified topic maps.
   *
   * @param topic topic name
   * @return AsyncLeaderElector to which topic maps
   */
  private AsyncLeaderElector<T> getLeaderElector(String topic) {
    return partitions.get(topicHasher.partition(topic));
  }

  /**
   * Returns all the constituent leader electors.
   *
   * @return collection of leader electors.
   */
  private Collection<AsyncLeaderElector<T>> getLeaderElectors() {
    return partitions.values();
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    partitions.values().forEach(elector -> elector.addStatusChangeListener(listener));
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    partitions.values().forEach(elector -> elector.removeStatusChangeListener(listener));
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    throw new UnsupportedOperationException();
  }

  @Override
  public LeaderElector<T> sync(Duration operationTimeout) {
    return new BlockingLeaderElector<>(this, operationTimeout.toMillis());
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(getLeaderElectors()
        .stream()
        .map(AsyncPrimitive::close)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }
}