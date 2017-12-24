/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.rest.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;

import io.atomix.core.PrimitivesService;
import io.atomix.primitive.AsyncPrimitive;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Primitive cache.
 */
public class PrimitiveCache {
  private final PrimitivesService primitives;
  private final Cache<String, AsyncPrimitive> cache;

  public PrimitiveCache(PrimitivesService primitives, int cacheSize) {
    this.primitives = primitives;
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .removalListener(this::closePrimitive)
        .build();
  }

  /**
   * Loads a cached asynchronous primitive.
   *
   * @param name the primitive name
   * @param factory the primitive factory
   * @param <T> the primitive type
   * @return a future to be completed with the opened primitive
   */
  @SuppressWarnings("unchecked")
  public <T extends AsyncPrimitive> T getPrimitive(String name, Function<PrimitivesService, T> factory) {
    try {
      return (T) cache.get(name, () -> factory.apply(primitives));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes a primitive when it's expired from the cache.
   */
  private void closePrimitive(RemovalNotification<String, ? extends AsyncPrimitive> notification) {
    notification.getValue().close();
  }
}
