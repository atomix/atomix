// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
