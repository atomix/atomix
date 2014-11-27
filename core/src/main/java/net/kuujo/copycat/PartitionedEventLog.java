/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.spi.Partitioner;

/**
 * Partitioned event log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PartitionedEventLog extends EventLog {

  /**
   * Returns a new partitioned event log.
   *
   * @return A new partitioned event log.
   */
  static PartitionedEventLog log() {
    return null;
  }

  /**
   * Returns a new partitioned event log.
   *
   * @param cluster The cluster configuration.
   * @param log The log configuration.
   * @return A new partitioned event log.
   */
  static PartitionedEventLog log(ClusterConfig cluster, LogConfig log) {
    return null;
  }

  /**
   * Sets the log partitioner.
   *
   * @param partitioner The event log partitioner.
   */
  void setPartitioner(Partitioner partitioner);

  /**
   * Returns the log partitioner.
   *
   * @return The event log partitioner.
   */
  Partitioner getPartitioner();

  /**
   * Sets the log partitioner, returning the event log for method chaining.
   *
   * @param partitioner The event log partitioner.
   * @return The partitioned event log.
   */
  PartitionedEventLog withPartitioner(Partitioner partitioner);

}
