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
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;

import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Primary-backup protocol configuration.
 */
public class PrimaryBackupProtocolConfig extends PrimitiveProtocolConfig<PrimaryBackupProtocolConfig> {
  private Consistency consistency = Consistency.SEQUENTIAL;
  private Replication replication = Replication.ASYNCHRONOUS;
  private Recovery recovery = Recovery.RECOVER;
  private int numBackups = 1;
  private int maxRetries = 0;
  private Duration retryDelay = Duration.ofMillis(100);
  private Executor executor;

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
  public PrimaryBackupProtocolConfig setConsistency(Consistency consistency) {
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
  public PrimaryBackupProtocolConfig setReplication(Replication replication) {
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
  public PrimaryBackupProtocolConfig setRecovery(Recovery recovery) {
    this.recovery = recovery;
    return this;
  }

  /**
   * Returns the number of backups.
   *
   * @return the number of backups
   */
  public int getNumBackups() {
    return numBackups;
  }

  /**
   * Sets the number of backups.
   *
   * @param numBackups the number of backups
   * @return the protocol configuration
   */
  public PrimaryBackupProtocolConfig setNumBackups(int numBackups) {
    this.numBackups = numBackups;
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
  public PrimaryBackupProtocolConfig setMaxRetries(int maxRetries) {
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
  public PrimaryBackupProtocolConfig setRetryDelayMillis(long retryDelayMillis) {
    return setRetryDelay(Duration.ofMillis(retryDelayMillis));
  }

  /**
   * Sets the retry delay.
   *
   * @param retryDelay the retry delay
   * @return the protocol configuration
   */
  public PrimaryBackupProtocolConfig setRetryDelay(Duration retryDelay) {
    this.retryDelay = retryDelay;
    return this;
  }

  /**
   * Returns the executor.
   *
   * @return the executor
   */
  public Executor getExecutor() {
    return executor;
  }

  /**
   * Sets the executor.
   *
   * @param executor the executor
   * @return the protocol configuration
   */
  public PrimaryBackupProtocolConfig setExecutor(Executor executor) {
    this.executor = executor;
    return this;
  }
}
