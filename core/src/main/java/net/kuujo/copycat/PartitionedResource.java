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
package net.kuujo.copycat;

import java.util.List;

/**
 * Partitioned resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PartitionedResource<T extends ResourcePartition> extends Resource {

  /**
   * Returns a list of resource partitions.
   *
   * @return A list of resource partitions.
   */
  List<T> partitions();

  /**
   * Returns a resource partition by partition number.
   *
   * @param partition The resource partition number.
   * @return The resource partition.
   * @throws java.lang.IndexOutOfBoundsException If the given {@code partition} is not a valid partition number
   *         for the resource
   */
  T partition(int partition);

}
