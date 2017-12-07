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
package io.atomix.protocols.raft;

import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Recovery;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft protocol.
 */
public class RaftProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type() {
  };

  /**
   * Returns a new Raft protocol builder.
   *
   * @return a new Raft protocol builder
   */
  public static Builder builder() {
    return builder(null);
  }

  /**
   * Returns a new Raft protocol builder.
   *
   * @param group the partition group
   * @return the Raft protocol builder
   */
  public static Builder builder(String group) {
    return new Builder(group);
  }

  private final String group;
  private final Duration minTimeout;
  private final Duration maxTimeout;
  private final ReadConsistency readConsistency;
  private final CommunicationStrategy communicationStrategy;
  private final Recovery recoveryStrategy;
  private final int maxRetries;
  private final Duration retryDelay;
  private final Executor executor;

  protected RaftProtocol(
      String group,
      Duration minTimeout,
      Duration maxTimeout,
      ReadConsistency readConsistency,
      CommunicationStrategy communicationStrategy,
      Recovery recoveryStrategy,
      int maxRetries,
      Duration retryDelay,
      Executor executor) {
    this.group = group;
    this.minTimeout = minTimeout;
    this.maxTimeout = maxTimeout;
    this.readConsistency = readConsistency;
    this.communicationStrategy = communicationStrategy;
    this.recoveryStrategy = recoveryStrategy;
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
   * Returns the minimum timeout.
   *
   * @return the minimum timeout
   */
  public Duration minTimeout() {
    return minTimeout;
  }

  /**
   * Returns the maximum timeout.
   *
   * @return the maximum timeout
   */
  public Duration maxTimeout() {
    return maxTimeout;
  }

  /**
   * Returns the read consistency.
   *
   * @return the read consistency
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  /**
   * Returns the communication strategy.
   *
   * @return the communication strategy
   */
  public CommunicationStrategy communicationStrategy() {
    return communicationStrategy;
  }

  /**
   * Returns the recovery strategy.
   *
   * @return the recovery strategy
   */
  public Recovery recoveryStrategy() {
    return recoveryStrategy;
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

  /**
   * Raft protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<RaftProtocol> {
    private Duration minTimeout = Duration.ofMillis(250);
    private Duration maxTimeout = Duration.ofSeconds(30);
    private ReadConsistency readConsistency = ReadConsistency.SEQUENTIAL;
    private CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    private Recovery recoveryStrategy = Recovery.RECOVER;
    private int maxRetries = 0;
    private Duration retryDelay = Duration.ofMillis(100);
    private Executor executor;

    protected Builder(String group) {
      super(group);
    }

    /**
     * Sets the minimum session timeout.
     *
     * @param minTimeout the minimum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMinTimeout(Duration minTimeout) {
      this.minTimeout = checkNotNull(minTimeout, "minTimeout cannot be null");
      return this;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param maxTimeout the maximum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMaxTimeout(Duration maxTimeout) {
      this.maxTimeout = checkNotNull(maxTimeout, "maxTimeout cannot be null");
      return this;
    }

    /**
     * Sets the read consistency level.
     *
     * @param readConsistency the read consistency level
     * @return the Raft protocol builder
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      this.readConsistency = checkNotNull(readConsistency, "readConsistency cannot be null");
      return this;
    }

    /**
     * Sets the communication strategy.
     *
     * @param communicationStrategy the communication strategy
     * @return the Raft protocol builder
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = checkNotNull(communicationStrategy, "communicationStrategy cannot be null");
      return this;
    }

    /**
     * Sets the recovery strategy.
     *
     * @param recoveryStrategy the recovery strategy
     * @return the Raft protocol builder
     */
    public Builder withRecoveryStrategy(Recovery recoveryStrategy) {
      this.recoveryStrategy = checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
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
     * @param timeUnit   the delay time unit
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
    public RaftProtocol build() {
      return new RaftProtocol(
          group,
          minTimeout,
          maxTimeout,
          readConsistency,
          communicationStrategy,
          recoveryStrategy,
          maxRetries,
          retryDelay,
          executor);
    }
  }
}
