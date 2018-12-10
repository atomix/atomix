/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.Murmur3Partitioner;
import io.atomix.protocols.backup.MultiPrimaryProtocolConfig;
import io.atomix.protocols.raft.MultiRaftProtocolConfig;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

/**
 * Benchmark serializer utilities.
 */
public final class BenchmarkSerializer {

  public static final Serializer INSTANCE = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(BenchmarkConfig.class)
      .register(BenchmarkProgress.class)
      .register(BenchmarkState.class)
      .register(BenchmarkResult.class)
      .register(RunnerProgress.class)
      .register(RunnerResult.class)
      .register(MultiRaftProtocolConfig.class)
      .register(Murmur3Partitioner.class)
      .register(ReadConsistency.class)
      .register(CommunicationStrategy.class)
      .register(Recovery.class)
      .register(MultiPrimaryProtocolConfig.class)
      .register(Consistency.class)
      .register(Replication.class)
      .build());

  private BenchmarkSerializer() {
  }
}
