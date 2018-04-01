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
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.utils.serializer.Serializer;

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
  public DistributedSetBuilder<E> withCacheEnabled(boolean cacheEnabled) {
    mapBuilder.withCacheEnabled(cacheEnabled);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withCacheSize(int cacheSize) {
    mapBuilder.withCacheSize(cacheSize);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withReadOnly(boolean readOnly) {
    mapBuilder.withReadOnly(readOnly);
    return this;
  }

  @Override
  public DistributedSetBuilder<E> withProtocol(PrimitiveProtocol protocol) {
    mapBuilder.withProtocol(protocol);
    return this;
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
  public Serializer serializer() {
    return mapBuilder.serializer();
  }

  @Override
  public boolean readOnly() {
    return mapBuilder.readOnly();
  }

  @Override
  public boolean cacheEnabled() {
    return mapBuilder.cacheEnabled();
  }

  @Override
  public int cacheSize() {
    return mapBuilder.cacheSize();
  }

  @Override
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    return mapBuilder.buildAsync()
        .thenApply(map -> new DelegatingAsyncDistributedSet<>(map.async()).sync());
  }
}
