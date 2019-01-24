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
package io.atomix.protocols.log;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Log protocol builder.
 */
public class DistributedLogProtocolBuilder extends PrimitiveProtocolBuilder<DistributedLogProtocolBuilder, DistributedLogProtocolConfig, DistributedLogProtocol> {
  protected DistributedLogProtocolBuilder(DistributedLogProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol builder
   */
  public DistributedLogProtocolBuilder withPartitioner(Partitioner<String> partitioner) {
    config.setPartitioner(partitioner);
    return this;
  }

  /**
   * Sets the protocol consistency model.
   *
   * @param consistency the protocol consistency model
   * @return the protocol builder
   */
  public DistributedLogProtocolBuilder withConsistency(Consistency consistency) {
    config.setConsistency(consistency);
    return this;
  }

  /**
   * Sets the protocol replication strategy.
   *
   * @param replication the protocol replication strategy
   * @return the protocol builder
   */
  public DistributedLogProtocolBuilder withReplication(Replication replication) {
    config.setReplication(replication);
    return this;
  }

  /**
   * Sets the protocol recovery strategy.
   *
   * @param recovery the protocol recovery strategy
   * @return the protocol builder
   */
  public DistributedLogProtocolBuilder withRecovery(Recovery recovery) {
    config.setRecovery(recovery);
    return this;
  }

  /**
   * Sets the maximum number of retries before an operation can be failed.
   *
   * @param maxRetries the maximum number of retries before an operation can be failed
   * @return the proxy builder
   */
  public DistributedLogProtocolBuilder withMaxRetries(int maxRetries) {
    config.setMaxRetries(maxRetries);
    return this;
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelayMillis the delay between operation retries in milliseconds
   * @return the proxy builder
   */
  public DistributedLogProtocolBuilder withRetryDelayMillis(long retryDelayMillis) {
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
  public DistributedLogProtocolBuilder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
    return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelay the delay between operation retries
   * @return the proxy builder
   * @throws NullPointerException if the delay is null
   */
  public DistributedLogProtocolBuilder withRetryDelay(Duration retryDelay) {
    config.setRetryDelay(retryDelay);
    return this;
  }

  @Override
  public DistributedLogProtocol build() {
    return new DistributedLogProtocol(config);
  }
}
