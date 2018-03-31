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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract builder for distributed primitives.
 *
 * @param <B> builder type
 * @param <P> primitive type
 */
public abstract class DistributedPrimitiveBuilder<B extends DistributedPrimitiveBuilder<B, P>, P extends DistributedPrimitive> implements Builder<P> {
  private final PrimitiveType type;
  private final String name;
  private Serializer serializer;
  private boolean readOnly = false;
  private boolean relaxedReadConsistency = false;
  private PrimitiveProtocol protocol;
  private int numBackups = 2;
  private int maxRetries;
  private Duration retryDelay = Duration.ofMillis(100);

  public DistributedPrimitiveBuilder(PrimitiveType type, String name) {
    this.type = checkNotNull(type, "type cannot be null");
    this.name = checkNotNull(name, "name cannot be null");
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
   * Disables state changing operations on the returned distributed primitive.
   *
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withUpdatesDisabled() {
    this.readOnly = true;
    return (B) this;
  }

  /**
   * Turns on relaxed consistency for read operations.
   *
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withRelaxedReadConsistency() {
    this.relaxedReadConsistency = true;
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
   * Returns if updates are disabled.
   *
   * @return {@code true} if yes; {@code false} otherwise
   */
  public boolean readOnly() {
    return readOnly;
  }

  /**
   * Returns if consistency is relaxed for read operations.
   *
   * @return {@code true} if yes; {@code false} otherwise
   */
  public boolean relaxedReadConsistency() {
    return relaxedReadConsistency;
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
