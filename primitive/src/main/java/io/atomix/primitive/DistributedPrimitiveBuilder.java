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
package io.atomix.primitive;

import io.atomix.utils.Builder;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract builder for distributed primitives.
 *
 * @param <B> builder type
 * @param <P> primitive type
 */
public abstract class DistributedPrimitiveBuilder<B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig, P extends DistributedPrimitive> implements Builder<P> {
  private static final int DEFAULT_CACHE_SIZE = 1000;
  private final PrimitiveType type;
  private final String name;
  protected final C config;
  private Serializer serializer;
  private PrimitiveProtocol protocol;
  private boolean cacheEnabled = false;
  private int cacheSize = DEFAULT_CACHE_SIZE;
  private boolean readOnly = false;

  public DistributedPrimitiveBuilder(PrimitiveType type, String name, C config) {
    this.type = checkNotNull(type, "type cannot be null");
    this.name = checkNotNull(name, "name cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
  }

  /**
   * Sets the serializer to use for transcoding info held in the primitive.
   *
   * @param serializer serializer
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withSerializer(Serializer serializer) {
    this.serializer = serializer;
    return (B) this;
  }

  /**
   * Sets the primitive protocol.
   *
   * @param protocol the primitive protocol
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withProtocol(PrimitiveProtocol protocol) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    return (B) this;
  }

  /**
   * Enables caching for the primitive.
   *
   * @return the primitive builder
   */
  public B withCacheEnabled() {
    return withCacheEnabled(true);
  }

  /**
   * Sets whether caching is enabled.
   *
   * @param cacheEnabled whether caching is enabled
   * @return the primitive builder
   */
  public B withCacheEnabled(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
    return (B) this;
  }

  /**
   * Sets the cache size.
   *
   * @param cacheSize the cache size
   * @return the primitive builder
   */
  public B withCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    return (B) this;
  }

  /**
   * Sets the primitive to read-only.
   *
   * @return the primitive builder
   */
  public B withReadOnly() {
    return withReadOnly(true);
  }

  /**
   * Sets whether the primitive is read-only.
   *
   * @param readOnly whether the primitive is read-only
   * @return the primitive builder
   */
  public B withReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return (B) this;
  }

  /**
   * Returns the name of the primitive.
   *
   * @return primitive name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the primitive type.
   *
   * @return primitive type
   */
  public PrimitiveType primitiveType() {
    return type;
  }

  /**
   * Returns the primitive protocol.
   *
   * @return the primitive protocol
   */
  public PrimitiveProtocol protocol() {
    return protocol;
  }

  /**
   * Returns the serializer.
   *
   * @return serializer
   */
  public Serializer serializer() {
    if (serializer == null) {
      serializer = Serializer.using(KryoNamespaces.BASIC);
    }
    return serializer;
  }

  /**
   * Returns whether caching is enabled.
   *
   * @return whether caching is enabled
   */
  public boolean cacheEnabled() {
    return cacheEnabled;
  }

  /**
   * Returns the cache size.
   *
   * @return the cache size
   */
  public int cacheSize() {
    return cacheSize;
  }

  /**
   * Returns whether the primitive is read-only.
   *
   * @return whether the primitive is read-only
   */
  public boolean readOnly() {
    return readOnly;
  }

  /**
   * Constructs an instance of the distributed primitive.
   *
   * @return distributed primitive
   */
  @Override
  public P build() {
    return buildAsync().join();
  }

  /**
   * Constructs an instance of the asynchronous primitive.
   *
   * @return asynchronous distributed primitive
   */
  public abstract CompletableFuture<P> buildAsync();
}
