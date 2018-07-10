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
