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
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RecoveryStrategy;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft protocol.
 */
public class RaftProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type() {};

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
  private final RecoveryStrategy recoveryStrategy;

  protected RaftProtocol(
      String group,
      Duration minTimeout,
      Duration maxTimeout,
      ReadConsistency readConsistency,
      CommunicationStrategy communicationStrategy,
      RecoveryStrategy recoveryStrategy) {
    this.group = group;
    this.minTimeout = minTimeout;
    this.maxTimeout = maxTimeout;
    this.readConsistency = readConsistency;
    this.communicationStrategy = communicationStrategy;
    this.recoveryStrategy = recoveryStrategy;
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
  public RecoveryStrategy recoveryStrategy() {
    return recoveryStrategy;
  }

  /**
   * Raft protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<RaftProtocol> {
    private Duration minTimeout = Duration.ofMillis(250);
    private Duration maxTimeout = Duration.ofSeconds(30);
    private ReadConsistency readConsistency = ReadConsistency.SEQUENTIAL;
    private CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    private RecoveryStrategy recoveryStrategy = RecoveryStrategy.RECOVER;

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
    public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
      this.recoveryStrategy = checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
      return this;
    }

    @Override
    public RaftProtocol build() {
      return new RaftProtocol(group, minTimeout, maxTimeout, readConsistency, communicationStrategy, recoveryStrategy);
    }
  }
}
