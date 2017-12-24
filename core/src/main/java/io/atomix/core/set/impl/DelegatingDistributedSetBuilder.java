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
package io.atomix.core.set.impl;

import io.atomix.core.map.ConsistentMapBuilder;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Replication;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DelegatingDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  private ConsistentMapBuilder<E, Boolean> mapBuilder;

  public DelegatingDistributedSetBuilder(ConsistentMapBuilder<E, Boolean> mapBuilder) {
    super(mapBuilder.name());
    this.mapBuilder = mapBuilder;
  }

  @Override
  public DistributedSetBuilder<E> withSerializer(Serializer serializer) {
    mapBuilder.withSerializer(serializer);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withUpdatesDisabled() {
    mapBuilder.withUpdatesDisabled();
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withRelaxedReadConsistency() {
    mapBuilder.withRelaxedReadConsistency();
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withProtocol(PrimitiveProtocol protocol) {
    mapBuilder.withProtocol(protocol);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withConsistency(Consistency consistency) {
    mapBuilder.withConsistency(consistency);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withPersistence(Persistence persistence) {
    mapBuilder.withPersistence(persistence);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withReplication(Replication replication) {
    mapBuilder.withReplication(replication);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withBackups(int numBackups) {
    mapBuilder.withBackups(numBackups);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withMaxRetries(int maxRetries) {
    mapBuilder.withMaxRetries(maxRetries);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withRetryDelay(Duration retryDelay) {
    mapBuilder.withRetryDelay(retryDelay);
    return this;
  }

  @Override
  public boolean readOnly() {
    return mapBuilder.readOnly();
  }

  @Override
  public boolean relaxedReadConsistency() {
    return mapBuilder.relaxedReadConsistency();
  }

  @Override
  public Serializer serializer() {
    return mapBuilder.serializer();
  }

  @Override
  public String name() {
    return mapBuilder.name();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return mapBuilder.protocol();
  }

  @Override
  public Consistency consistency() {
    return mapBuilder.consistency();
  }

  @Override
  public Persistence persistence() {
    return mapBuilder.persistence();
  }

  @Override
  public Replication replication() {
    return mapBuilder.replication();
  }

  @Override
  public int backups() {
    return mapBuilder.backups();
  }

  @Override
  public int maxRetries() {
    return mapBuilder.maxRetries();
  }

  @Override
  public Duration retryDelay() {
    return mapBuilder.retryDelay();
  }

  @Override
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    return mapBuilder.buildAsync()
        .thenApply(map -> new DelegatingAsyncDistributedSet<>(map.async()).sync());
  }
}
