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
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Multi-primary protocol builder.
 */
public class MultiPrimaryProtocolBuilder extends PrimitiveProtocolBuilder<MultiPrimaryProtocolBuilder, MultiPrimaryProtocolConfig, MultiPrimaryProtocol> {
  protected MultiPrimaryProtocolBuilder(MultiPrimaryProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol builder
   */
  public MultiPrimaryProtocolBuilder withPartitioner(Partitioner<String> partitioner) {
    config.setPartitioner(partitioner);
    return this;
  }

  /**
   * Sets the protocol consistency model.
   *
   * @param consistency the protocol consistency model
   * @return the protocol builder
   */
  public MultiPrimaryProtocolBuilder withConsistency(Consistency consistency) {
    config.setConsistency(consistency);
    return this;
  }

  /**
   * Sets the protocol replication strategy.
   *
   * @param replication the protocol replication strategy
   * @return the protocol builder
   */
  public MultiPrimaryProtocolBuilder withReplication(Replication replication) {
    config.setReplication(replication);
    return this;
  }

  /**
   * Sets the protocol recovery strategy.
   *
   * @param recovery the protocol recovery strategy
   * @return the protocol builder
   */
  public MultiPrimaryProtocolBuilder withRecovery(Recovery recovery) {
    config.setRecovery(recovery);
    return this;
  }

  /**
   * Sets the number of backups.
   *
   * @param numBackups the number of backups
   * @return the protocol builder
   */
  public MultiPrimaryProtocolBuilder withBackups(int numBackups) {
    config.setBackups(numBackups);
    return this;
  }

  /**
   * Sets the maximum number of retries before an operation can be failed.
   *
   * @param maxRetries the maximum number of retries before an operation can be failed
   * @return the proxy builder
   */
  public MultiPrimaryProtocolBuilder withMaxRetries(int maxRetries) {
    config.setMaxRetries(maxRetries);
    return this;
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelayMillis the delay between operation retries in milliseconds
   * @return the proxy builder
   */
  public MultiPrimaryProtocolBuilder withRetryDelayMillis(long retryDelayMillis) {
    config.setRetryDelayMillis(retryDelayMillis);
    return this;
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelay the delay between operation retries
   * @param timeUnit   the delay time unit
   * @return the proxy builder
   * @throws NullPointerException if the time unit is null
   */
  public MultiPrimaryProtocolBuilder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
    return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelay the delay between operation retries
   * @return the proxy builder
   * @throws NullPointerException if the delay is null
   */
  public MultiPrimaryProtocolBuilder withRetryDelay(Duration retryDelay) {
    config.setRetryDelay(retryDelay);
    return this;
  }

  @Override
  public MultiPrimaryProtocol build() {
    return new MultiPrimaryProtocol(config);
  }
}
