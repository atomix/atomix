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
package io.atomix.primitives.set.impl;

import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.set.AsyncDistributedSet;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.serializer.Serializer;

import java.util.function.Supplier;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DefaultDistributedSetBuilder<E> extends DistributedSetBuilder<E> {

  private ConsistentMapBuilder<E, Boolean> mapBuilder;

  public DefaultDistributedSetBuilder(Supplier<ConsistentMapBuilder<E, Boolean>> mapBuilderSupplier) {
    this.mapBuilder = mapBuilderSupplier.get();
  }

  @Override
  public DistributedSetBuilder<E> withName(String name) {
    mapBuilder.withName(name);
    return this;
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
  public AsyncDistributedSet<E> buildAsync() {
    return new DelegatingAsyncDistributedSet<>(mapBuilder.buildAsync());
  }
}
