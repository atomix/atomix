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
package io.atomix.primitives;

import io.atomix.serializer.Serializer;
import io.atomix.utils.Builder;

/**
 * Abstract builder for distributed primitives.
 *
 * @param <B> builder type
 * @param <S> synchronous primitive type
 * @param <A> asynchronous primitive type
 */
public abstract class DistributedPrimitiveBuilder<B extends DistributedPrimitiveBuilder<B, S, A>, S extends SyncPrimitive, A extends AsyncPrimitive> implements Builder<S> {

  private final DistributedPrimitive.Type type;
  private String name;
  private Serializer serializer;
  private boolean readOnly = false;
  private boolean relaxedReadConsistency = false;

  public DistributedPrimitiveBuilder(DistributedPrimitive.Type type) {
    this.type = type;
  }

  /**
   * Sets the primitive name.
   *
   * @param name primitive name
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withName(String name) {
    this.name = name;
    return (B) this;
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
  public DistributedPrimitive.Type primitiveType() {
    return type;
  }

  /**
   * Constructs an instance of the distributed primitive.
   *
   * @return distributed primitive
   */
  @Override
  public abstract S build();

  /**
   * Constructs an instance of the asynchronous primitive.
   *
   * @return asynchronous distributed primitive
   */
  public abstract A buildAsync();
}
