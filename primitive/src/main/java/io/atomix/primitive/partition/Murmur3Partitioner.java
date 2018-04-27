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
package io.atomix.primitive.partition;

import com.google.common.hash.Hashing;

import java.util.List;

/**
 * Murmur 3 partitioner.
 */
public class Murmur3Partitioner implements Partitioner<String> {
  @Override
  public PartitionId partition(String key, List<PartitionId> partitions) {
    int hash = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt());
    return partitions.get(Hashing.consistentHash(hash, partitions.size()));
  }
}
