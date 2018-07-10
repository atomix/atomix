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
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

import java.time.Duration;

/**
 * Multi-primary protocol configuration.
 */
public class MultiPrimaryProtocolConfig extends PrimitiveProtocolConfig<MultiPrimaryProtocolConfig> {
  private String group;
  private Partitioner<String> partitioner = Partitioner.MURMUR3;
  private Consistency consistency = Consistency.SEQUENTIAL;
  private Replication replication = Replication.ASYNCHRONOUS;
  private Recovery recovery = Recovery.RECOVER;
  private int backups = 1;
  private int maxRetries = 0;
  private Duration retryDelay = Duration.ofMillis(100);

  @Override
  public PrimitiveProtocol.Type getType() {
    return MultiPrimaryProtocol.TYPE;
  }

  /**
   * Returns the partition group.
   *
   * @return the partition group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Sets the partition group.
   *
   * @param group the partition group
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setGroup(String group) {
    this.group = group;
    return this;
  }

  /**
   * Returns the protocol partitioner.
   *
   * @return the protocol partitioner
   */
  public Partitioner<String> getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setPartitioner(Partitioner<String> partitioner) {
    this.partitioner = partitioner;
    return this;
  }

  /**
   * Returns the consistency level.
   *
   * @return the consistency level
   */
  public Consistency getConsistency() {
    return consistency;
  }

  /**
   * Sets the consistency level.
   *
   * @param consistency the consistency level
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Returns the replication level.
   *
   * @return the replication level
   */
  public Replication getReplication() {
    return replication;
  }

  /**
   * Sets the replication level.
   *
   * @param replication the replication level
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setReplication(Replication replication) {
    this.replication = replication;
    return this;
  }

  /**
   * Returns the recovery strategy.
   *
   * @return the recovery strategy
   */
  public Recovery getRecovery() {
    return recovery;
  }

  /**
   * Sets the recovery strategy.
   *
   * @param recovery the recovery strategy
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setRecovery(Recovery recovery) {
    this.recovery = recovery;
    return this;
  }

  /**
   * Returns the number of backups.
   *
   * @return the number of backups
   */
  public int getBackups() {
    return backups;
  }

  /**
   * Sets the number of backups.
   *
   * @param numBackups the number of backups
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setBackups(int numBackups) {
    this.backups = numBackups;
    return this;
  }

  /**
   * Returns the maximum allowed number of retries.
   *
   * @return the maximum allowed number of retries
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * Sets the maximum allowed number of retries.
   *
   * @param maxRetries the maximum allowed number of retries
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * Returns the retry delay.
   *
   * @return the retry delay
   */
  public Duration getRetryDelay() {
    return retryDelay;
  }

  /**
   * Sets the retry delay.
   *
   * @param retryDelayMillis the retry delay in milliseconds
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setRetryDelayMillis(long retryDelayMillis) {
    return setRetryDelay(Duration.ofMillis(retryDelayMillis));
  }

  /**
   * Sets the retry delay.
   *
   * @param retryDelay the retry delay
   * @return the protocol configuration
   */
  public MultiPrimaryProtocolConfig setRetryDelay(Duration retryDelay) {
    this.retryDelay = retryDelay;
    return this;
  }
}
