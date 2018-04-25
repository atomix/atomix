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
package io.atomix.protocols.raft;

import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-Raft protocol configuration.
 */
public class MultiRaftProtocolConfig extends RaftProtocolConfig {
  private Partitioner<String> partitioner = Partitioner.MURMUR3;

  @Override
  public PrimitiveProtocol.Type getType() {
    return MultiRaftProtocol.TYPE;
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
  public MultiRaftProtocolConfig setPartitioner(Partitioner<String> partitioner) {
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setGroup(String group) {
    super.setGroup(group);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setMinTimeout(Duration minTimeout) {
    super.setMinTimeout(minTimeout);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setMaxTimeout(Duration maxTimeout) {
    super.setMaxTimeout(maxTimeout);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setReadConsistency(ReadConsistency readConsistency) {
    super.setReadConsistency(readConsistency);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setCommunicationStrategy(CommunicationStrategy communicationStrategy) {
    super.setCommunicationStrategy(communicationStrategy);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setRecoveryStrategy(Recovery recoveryStrategy) {
    super.setRecoveryStrategy(recoveryStrategy);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setMaxRetries(int maxRetries) {
    super.setMaxRetries(maxRetries);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setRetryDelayMillis(long retryDelayMillis) {
    super.setRetryDelayMillis(retryDelayMillis);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setRetryDelay(Duration retryDelay) {
    super.setRetryDelay(retryDelay);
    return this;
  }

  @Override
  public MultiRaftProtocolConfig setExecutor(Executor executor) {
    super.setExecutor(executor);
    return this;
  }
}
