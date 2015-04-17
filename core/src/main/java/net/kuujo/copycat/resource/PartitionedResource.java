/*
 * Copyright 2015 the original author or authors.
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

import java.util.List;

/**
 * Copycat resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PartitionedResource<T extends PartitionedResource<T, U>, U extends Partition<U>> extends Resource<T> {

  /**
   * Returns a list of resource partitions.
   *
   * @return A list of resource partitions.
   */
  List<U> partitions();

  /**
   * Returns a resource partition by ID.
   *
   * @param id The resource partition ID.
   * @return The resource partition.
   * @throws java.lang.IndexOutOfBoundsException If the partition number is not valid.
   */
  U partition(int id);

}
