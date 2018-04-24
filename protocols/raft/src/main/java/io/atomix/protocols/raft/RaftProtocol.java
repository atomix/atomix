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

import io.atomix.primitive.Recovery;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft protocol.
 */
public class RaftProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type();

  /**
   * The primary-backup protocol type.
   */
  public static class Type implements PrimitiveProtocol.Type {
    private static final String NAME = "raft";

    @Override
    public String name() {
      return NAME;
    }
  }

  /**
   * Returns a new Raft protocol builder.
   *
   * @return a new Raft protocol builder
   */
  public static Builder builder() {
    return new Builder(new RaftProtocolConfig());
  }

  /**
   * Returns a new Raft protocol builder.
   *
   * @param group the partition group
   * @return the Raft protocol builder
   */
  public static Builder builder(String group) {
    return new Builder(new RaftProtocolConfig().setGroup(group));
  }

  private final RaftProtocolConfig config;

  protected RaftProtocol(RaftProtocolConfig config) {
    this.config = checkNotNull(config, "config cannot be null");
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  /**
   * Returns the minimum timeout.
   *
   * @return the minimum timeout
   */
  public Duration minTimeout() {
    return config.getMinTimeout();
  }

  /**
   * Returns the maximum timeout.
   *
   * @return the maximum timeout
   */
  public Duration maxTimeout() {
    return config.getMaxTimeout();
  }

  /**
   * Returns the read consistency.
   *
   * @return the read consistency
   */
  public ReadConsistency readConsistency() {
    return config.getReadConsistency();
  }

  /**
   * Returns the communication strategy.
   *
   * @return the communication strategy
   */
  public CommunicationStrategy communicationStrategy() {
    return config.getCommunicationStrategy();
  }

  /**
   * Returns the recovery strategy.
   *
   * @return the recovery strategy
   */
  public Recovery recoveryStrategy() {
    return config.getRecoveryStrategy();
  }

  /**
   * Returns the maximum number of allowed retries.
   *
   * @return the maximum number of allowed retries
   */
  public int maxRetries() {
    return config.getMaxRetries();
  }

  /**
   * Returns the retry delay.
   *
   * @return the retry delay
   */
  public Duration retryDelay() {
    return config.getRetryDelay();
  }

  /**
   * Returns the proxy executor.
   *
   * @return the proxy executor
   */
  public Executor executor() {
    return config.getExecutor();
  }

  /**
   * Raft protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<RaftProtocolConfig, RaftProtocol> {
    protected Builder(RaftProtocolConfig config) {
      super(config);
    }

    /**
     * Sets the minimum session timeout.
     *
     * @param minTimeout the minimum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMinTimeout(Duration minTimeout) {
      config.setMinTimeout(minTimeout);
      return this;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param maxTimeout the maximum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMaxTimeout(Duration maxTimeout) {
      config.setMaxTimeout(maxTimeout);
      return this;
    }

    /**
     * Sets the read consistency level.
     *
     * @param readConsistency the read consistency level
     * @return the Raft protocol builder
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      config.setReadConsistency(readConsistency);
      return this;
    }

    /**
     * Sets the communication strategy.
     *
     * @param communicationStrategy the communication strategy
     * @return the Raft protocol builder
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      config.setCommunicationStrategy(communicationStrategy);
      return this;
    }

    /**
     * Sets the recovery strategy.
     *
     * @param recoveryStrategy the recovery strategy
     * @return the Raft protocol builder
     */
    public Builder withRecoveryStrategy(Recovery recoveryStrategy) {
      config.setRecoveryStrategy(recoveryStrategy);
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      config.setMaxRetries(maxRetries);
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
      config.setRetryDelay(retryDelay);
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
      config.setExecutor(executor);
      return this;
    }

    @Override
    public RaftProtocol build() {
      return new RaftProtocol(config);
    }
  }
}
