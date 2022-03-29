// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.cache;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;

/**
 * Cached distributed primitive builder.
 */
public abstract class CachedPrimitiveBuilder<B extends CachedPrimitiveBuilder<B, C, P>, C extends CachedPrimitiveConfig<C>, P extends SyncPrimitive>
    extends PrimitiveBuilder<B, C, P> {
  protected CachedPrimitiveBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }

  /**
   * Enables caching for the primitive.
   *
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheEnabled() {
    config.getCacheConfig().setCacheEnabled();
    return (B) this;
  }

  /**
   * Sets whether caching is enabled.
   *
   * @param cacheEnabled whether caching is enabled
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheEnabled(boolean cacheEnabled) {
    config.getCacheConfig().setEnabled(cacheEnabled);
    return (B) this;
  }

  /**
   * Sets the cache size.
   *
   * @param cacheSize the cache size
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withCacheSize(int cacheSize) {
    config.getCacheConfig().setSize(cacheSize);
    return (B) this;
  }
}
