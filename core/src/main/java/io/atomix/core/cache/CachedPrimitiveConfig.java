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
package io.atomix.core.cache;

import io.atomix.primitive.config.PrimitiveConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cached primitive configuration.
 */
public abstract class CachedPrimitiveConfig<C extends CachedPrimitiveConfig<C>> extends PrimitiveConfig<C> {
  private CacheConfig cacheConfig = new CacheConfig();

  /**
   * Returns the cache configuration.
   *
   * @return the cache configuration
   */
  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  /**
   * Sets the cache configuration.
   *
   * @param cacheConfig the cache configuration
   * @return the primitive configuration
   * @throws NullPointerException if the cache configuration is null
   */
  public C setCacheConfig(CacheConfig cacheConfig) {
    this.cacheConfig = checkNotNull(cacheConfig);
    return (C) this;
  }
}
