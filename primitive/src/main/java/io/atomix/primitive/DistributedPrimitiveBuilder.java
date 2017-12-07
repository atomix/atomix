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

import static com.google.common.base.Preconditions.checkArgument;
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
  private Persistence persistence = defaultPersistence();
  private Consistency consistency = defaultConsistency();
  private Replication replication = defaultReplication();
  private Recovery recovery = defaultRecovery();
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
   * Sets the primitive consistency model.
   *
   * @param consistency the primitive consistency model
   * @return the primitive builder
   * @throws NullPointerException if the consistency model is null
   */
  @SuppressWarnings("unchecked")
  public B withConsistency(Consistency consistency) {
    this.consistency = checkNotNull(consistency, "consistency cannot be null");
    return (B) this;
  }

  /**
   * Sets the primitive persistence level.
   *
   * @param persistence the primitive persistence level
   * @return the primitive builder
   * @throws NullPointerException if the persistence level is null
   */
  @SuppressWarnings("unchecked")
  public B withPersistence(Persistence persistence) {
    this.persistence = checkNotNull(persistence, "persistence cannot be null");
    return (B) this;
  }

  /**
   * Sets the primitive replication strategy.
   *
   * @param replication the primitive replication strategy
   * @return the primitive builder
   * @throws NullPointerException if the replication strategy is null
   */
  @SuppressWarnings("unchecked")
  public B withReplication(Replication replication) {
    this.replication = checkNotNull(replication, "replication cannot be null");
    return (B) this;
  }

  /**
   * Sets the primitive recovery strategy.
   *
   * @param recovery the primitive recovery strategy
   * @return the primitive builder
   * @throws NullPointerException if the recovery strategy is null
   */
  @SuppressWarnings("unchecked")
  public B withRecovery(Recovery recovery) {
    this.recovery = checkNotNull(recovery, "recovery cannot be null");
    return (B) this;
  }

  /**
   * Sets the number of backups.
   *
   * @param numBackups the number of backups
   * @return the primitive builder
   * @throws IllegalArgumentException if the number of backups is not positive or {@code -1}
   */
  @SuppressWarnings("unchecked")
  public B withBackups(int numBackups) {
    checkArgument(numBackups >= 0, "numBackups must be positive");
    this.numBackups = numBackups;
    return (B) this;
  }

  /**
   * Sets the maximum number of operation retries.
   *
   * @param maxRetries the maximum number of allowed operation retries
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be positive");
    this.maxRetries = maxRetries;
    return (B) this;
  }

  /**
   * Sets the retry delay.
   *
   * @param retryDelay the retry delay
   * @return this builder
   */
  @SuppressWarnings("unchecked")
  public B withRetryDelay(Duration retryDelay) {
    this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
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
   * Returns the primitive consistency model.
   *
   * @return the primitive consistency model
   */
  public Consistency consistency() {
    return consistency;
  }

  /**
   * Returns the default consistency model.
   *
   * @return the default consistency model
   */
  protected abstract Consistency defaultConsistency();

  /**
   * Returns the primitive persistence level.
   *
   * @return the primitive persistence level
   */
  public Persistence persistence() {
    return persistence;
  }

  /**
   * Returns the default persistence level.
   *
   * @return the default persistence level
   */
  protected abstract Persistence defaultPersistence();

  /**
   * Returns the replication strategy.
   *
   * @return the replication strategy
   */
  public Replication replication() {
    return replication;
  }

  /**
   * Returns the recovery strategy.
   *
   * @return the recovery strategy
   */
  public Recovery recovery() {
    return recovery;
  }

  /**
   * Returns the default recovery strategy.
   *
   * @return the default recovery strategy
   */
  protected Recovery defaultRecovery() {
    return Recovery.RECOVER;
  }

  /**
   * Returns the number of backups for the primitive.
   *
   * @return the number of backups for the primitive
   */
  public int backups() {
    return numBackups;
  }

  /**
   * Returns the maximum number of allowed retries.
   *
   * @return the maximum number of allowed retries
   */
  public int maxRetries() {
    return maxRetries;
  }

  /**
   * Returns the retry delay.
   *
   * @return the retry delay
   */
  public Duration retryDelay() {
    return retryDelay;
  }

  /**
   * Returns the default replication strategy.
   *
   * @return the default replication strategy
   */
  protected abstract Replication defaultReplication();

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
