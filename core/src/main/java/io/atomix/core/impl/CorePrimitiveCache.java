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
package io.atomix.core.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveCache;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Core primitive cache.
 */
public class CorePrimitiveCache implements PrimitiveCache {
  private final Map<String, CompletableFuture> primitives = Maps.newConcurrentMap();

  @Override
  @SuppressWarnings("unchecked")
  public <P extends DistributedPrimitive> CompletableFuture<P> getPrimitive(String name, Supplier<CompletableFuture<P>> supplier) {
    return primitives.computeIfAbsent(name, n -> supplier.get());
  }
}
