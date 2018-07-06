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
package io.atomix.core.map;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;

import java.time.Duration;

/**
 * Raft distributed map test.
 */
public class RaftDistributedMapTest extends DistributedMapTest {
  @Override
  protected PrimitiveProtocol protocol() {
    return MultiRaftProtocol.builder()
        .withMaxTimeout(Duration.ofSeconds(1))
        .withMaxRetries(5)
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .build();
  }
}
