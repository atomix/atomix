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

import java.time.Duration;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-primary protocol configuration.
 */
public class MultiPrimaryProtocolConfig extends PrimaryBackupProtocolConfig {
  private Partitioner<String> partitioner = Partitioner.MURMUR3;

  @Override
  public PrimitiveProtocol.Type getType() {
    return MultiPrimaryProtocol.TYPE;
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
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setGroup(String group) {
    super.setGroup(group);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setConsistency(Consistency consistency) {
    super.setConsistency(consistency);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setReplication(Replication replication) {
    super.setReplication(replication);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setRecovery(Recovery recovery) {
    super.setRecovery(recovery);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setNumBackups(int numBackups) {
    super.setNumBackups(numBackups);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setMaxRetries(int maxRetries) {
    super.setMaxRetries(maxRetries);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setRetryDelayMillis(long retryDelayMillis) {
    super.setRetryDelayMillis(retryDelayMillis);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setRetryDelay(Duration retryDelay) {
    super.setRetryDelay(retryDelay);
    return this;
  }

  @Override
  public MultiPrimaryProtocolConfig setExecutor(Executor executor) {
    super.setExecutor(executor);
    return this;
  }
}
