// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
