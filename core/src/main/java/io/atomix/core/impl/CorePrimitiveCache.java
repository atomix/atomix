// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
