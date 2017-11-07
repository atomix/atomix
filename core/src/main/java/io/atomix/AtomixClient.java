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
package io.atomix;

import io.atomix.partition.impl.AbstractPartition;
import io.atomix.partition.impl.ClientPartition;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Atomix client.
 */
public class AtomixClient extends Atomix {

  /**
   * Returns a new Atomix client builder.
   *
   * @return a new Atomix client builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public AtomixClient(Collection<AbstractPartition> partitions, int numBuckets) {
    super(partitions, numBuckets);
  }

  /**
   * Atomix client builder.
   */
  public static class Builder extends Atomix.Builder {
    @Override
    public Atomix build() {
      Collection<AbstractPartition> partitions = buildPartitionInfo(nodes, numPartitions, partitionSize)
          .stream()
          .map(p -> new ClientPartition(nodeId, p, clusterCommunicator))
          .collect(Collectors.toList());
      return new AtomixClient(partitions, numBuckets);
    }
  }
}
