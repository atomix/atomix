/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.partition;

import io.atomix.catalyst.util.Assert;

import java.util.Iterator;
import java.util.List;

/**
 * Group partitions.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Partitions implements Iterable<Partition> {
  final List<Partition> partitions;
  private final Partitioner partitioner;

  Partitions(List<Partition> partitions, Partitioner partitioner) {
    this.partitions = Assert.notNull(partitions, "partitions");
    this.partitioner = Assert.notNull(partitioner, "partitioner");
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId The partition ID.
   * @return The group partition.
   * @throws IndexOutOfBoundsException if the given {@code partitionId} is greater than the range of partitions in the group
   */
  public Partition get(int partitionId) {
    return partitions.get(partitionId);
  }

  /**
   * Returns a partition for the given value.
   *
   * @param value The value for which to return a partition.
   * @return The partition for the given value or {@code null} if the value was not mapped to any partition.
   */
  public Partition get(Object value) {
    int partitionId = partitioner.partition(value, partitions.size());
    return partitionId != -1 ? partitions.get(partitionId) : null;
  }

  /**
   * Returns the partitions size.
   *
   * @return The partitions size.
   */
  public int size() {
    return partitions.size();
  }

  @Override
  public Iterator<Partition> iterator() {
    return partitions.iterator();
  }

  @Override
  public String toString() {
    return String.format("%s[partitions=%d]", getClass().getSimpleName(), partitions.size());
  }

}
