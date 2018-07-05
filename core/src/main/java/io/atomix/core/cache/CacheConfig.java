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

import io.atomix.utils.config.Config;

/**
 * Cached primitive configuration.
 */
public class CacheConfig implements Config {
  private static final int DEFAULT_CACHE_SIZE = 1000;

  private boolean enabled = false;
  private int size = DEFAULT_CACHE_SIZE;

  /**
   * Enables caching for the primitive.
   *
   * @return the primitive configuration
   */
  public CacheConfig setCacheEnabled() {
    return setEnabled(true);
  }

  /**
   * Sets whether caching is enabled.
   *
   * @param enabled whether caching is enabled
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public CacheConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Returns whether caching is enabled.
   *
   * @return whether caching is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Sets the cache size.
   *
   * @param size the cache size
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public CacheConfig setSize(int size) {
    this.size = size;
    return this;
  }

  /**
   * Returns the cache size.
   *
   * @return the cache size
   */
  public int getSize() {
    return size;
  }

}
