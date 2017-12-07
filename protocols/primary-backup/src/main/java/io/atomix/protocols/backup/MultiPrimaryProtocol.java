/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type() {};

  /**
   * Returns a new multi-primary protocol builder.
   *
   * @return a new multi-primary protocol builder
   */
  public static Builder builder() {
    return builder(null);
  }

  /**
   * Returns a new multi-primary protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new multi-primary protocol builder for the given group
   */
  public static Builder builder(String group) {
    return new Builder(group);
  }

  private final String group;
  private final Consistency consistency;
  private final Replication replication;
  private final Recovery recovery;
  private final int backups;
  private final int maxRetries;
  private final Duration retryDelay;
  private final Executor executor;

  protected MultiPrimaryProtocol(
      String group,
      Consistency consistency,
      Replication replication,
      Recovery recovery,
      int backups,
      int maxRetries,
      Duration retryDelay,
      Executor executor) {
    this.group = group;
    this.consistency = consistency;
    this.replication = replication;
    this.recovery = recovery;
    this.backups = backups;
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
    this.executor = executor;
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return group;
  }

  /**
   * Returns the protocol consistency model.
   *
   * @return the protocol consistency model
   */
  public Consistency consistency() {
    return consistency;
  }

  /**
   * Returns the protocol replications strategy.
   *
   * @return the protocol replication strategy
   */
  public Replication replication() {
    return replication;
  }

  /**
   * Returns the protocol recovery strategy.
   *
   * @return the protocol recovery strategy
   */
  public Recovery recovery() {
    return recovery;
  }

  /**
   * Returns the number of backups.
   *
   * @return the number of backups
   */
  public int backups() {
    return backups;
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
   * Returns the proxy executor.
   *
   * @return the proxy executor
   */
  public Executor executor() {
    return executor;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("group", group())
        .add("consistency", consistency())
        .add("replication", replication())
        .add("backups", backups())
        .add("maxRetries", maxRetries)
        .add("retryDelay", retryDelay)
        .toString();
  }

  /**
   * Multi-primary protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder {
    private Consistency consistency = Consistency.SEQUENTIAL;
    private Replication replication = Replication.SYNCHRONOUS;
    private Recovery recovery = Recovery.RECOVER;
    private int numBackups;
    private int maxRetries = 0;
    private Duration retryDelay = Duration.ofMillis(100);
    private Executor executor;

    protected Builder(String group) {
      super(group);
    }

    /**
     * Sets the protocol consistency model.
     *
     * @param consistency the protocol consistency model
     * @return the protocol builder
     */
    public Builder withConsistency(Consistency consistency) {
      this.consistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    /**
     * Sets the protocol replication strategy.
     *
     * @param replication the protocol replication strategy
     * @return the protocol builder
     */
    public Builder withReplication(Replication replication) {
      this.replication = checkNotNull(replication, "replication cannot be null");
      return this;
    }

    /**
     * Sets the protocol recovery strategy.
     *
     * @param recovery the protocol recovery strategy
     * @return the protocol builder
     */
    public Builder withRecovery(Recovery recovery) {
      this.recovery = checkNotNull(recovery, "recovery cannot be null");
      return this;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol builder
     */
    public Builder withBackups(int numBackups) {
      checkArgument(numBackups >= 0, "numBackups must be positive");
      this.numBackups = numBackups;
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "maxRetries must be positive");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return withRetryDelay(Duration.ofMillis(retryDelayMillis));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
      return this;
    }

    /**
     * Sets the executor with which to complete proxy futures.
     *
     * @param executor The executor with which to complete proxy futures.
     * @return The proxy builder.
     * @throws NullPointerException if the executor is null
     */
    public Builder withExecutor(Executor executor) {
      this.executor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    @Override
    public MultiPrimaryProtocol build() {
      return new MultiPrimaryProtocol(
          group,
          consistency,
          replication,
          recovery,
          numBackups,
          maxRetries,
          retryDelay,
          executor);
    }
  }
}
