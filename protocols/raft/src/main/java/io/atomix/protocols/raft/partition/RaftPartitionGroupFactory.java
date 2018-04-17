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
package io.atomix.protocols.raft.partition;

import io.atomix.primitive.partition.PartitionGroupFactory;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.storage.StorageLevel;

import java.io.File;

/**
 * Raft partition group factory.
 */
public class RaftPartitionGroupFactory implements PartitionGroupFactory<RaftPartitionGroupConfig, RaftPartitionGroup> {
  private static final String SYSTEM_GROUP_NAME = "system";

  @Override
  public PrimitiveProtocol.Type type() {
    return RaftProtocol.TYPE;
  }

  @Override
  public RaftPartitionGroup createGroup(RaftPartitionGroupConfig config) {
    return new RaftPartitionGroup(config);
  }

  @Override
  public RaftPartitionGroup createSystemGroup(int size, File dataDirectory) {
    return RaftPartitionGroup.builder(SYSTEM_GROUP_NAME)
        .withNumPartitions(1)
        .withPartitionSize(size)
        .withDataDirectory(new File(dataDirectory, SYSTEM_GROUP_NAME))
        .withStorageLevel(StorageLevel.DISK)
        .build();
  }
}
