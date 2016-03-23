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
package io.atomix.group.util;

import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.group.partition.Partitioner;

/**
 * Group ID generator.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupIdGenerator {

  /**
   * Returns a hash code for the given hash group arguments.
   */
  public static int groupIdFor(int parent, Hasher hasher, int virtualNodes) {
    int hashCode = 17;
    hashCode = 37 * hashCode + parent;
    hashCode = 37 * hashCode + hasher.hashCode();
    hashCode = 37 * hashCode + virtualNodes;
    return hashCode;
  }

  /**
   * Calculates a hash code for the given group arguments.
   */
  public static int groupIdFor(int parent, int partitions, int replicationFactor, Partitioner partitioner) {
    int hashCode = 31;
    hashCode = 37 * hashCode + parent;
    hashCode = 37 * hashCode + partitions;
    hashCode = 37 * hashCode + replicationFactor;
    hashCode = 37 * hashCode + partitioner.hashCode();
    return hashCode;
  }

  private GroupIdGenerator() {
  }

}
