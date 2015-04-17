/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.util.Managed;

/**
 * Copycat resource partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Partition<T extends Partition<T>> extends Managed<T> {

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  String name();

  /**
   * Returns the partition identifier.
   * <p>
   * The partition ID is a zero-based monotonically increasing number that is unique to each partition of a given resource.
   *
   * @return The partition identifier.
   */
  int partition();

  /**
   * Returns the current resource status.<p>
   *
   * All resources begin in the {@link PartitionState#RECOVER} status. Once the Raft algorithm has caught up to the
   * leader's commit index at the time the resource was opened the resource will transition to the
   * {@link PartitionState#HEALTHY} status.
   *
   * @return The current resource status.
   */
  PartitionState state();

  /**
   * Returns the resource cluster.
   *
   * @return The resource cluster.
   */
  Cluster cluster();

}
