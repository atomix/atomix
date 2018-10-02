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
package io.atomix.core.registry;

import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.profile.ConsensusProfile;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Static registry test.
 */
public class SimpleRegistryTest {
  @Test
  public void testStaticRegistryBuilder() throws Exception {
    AtomixRegistry registry = SimpleRegistry.builder()
        .addProfileType(ConsensusProfile.TYPE)
        .addDiscoveryProviderType(BootstrapDiscoveryProvider.TYPE)
        .addPrimitiveType(AtomicCounterType.instance())
        .addProtocolType(MultiRaftProtocol.TYPE)
        .addPartitionGroupType(RaftPartitionGroup.TYPE)
        .build();

    assertEquals(ConsensusProfile.TYPE, registry.getType(Profile.Type.class, "consensus"));
    assertEquals(BootstrapDiscoveryProvider.TYPE, registry.getType(NodeDiscoveryProvider.Type.class, "bootstrap"));
    assertEquals(AtomicCounterType.instance(), registry.getType(PrimitiveType.class, "atomic-counter"));
    assertEquals(MultiRaftProtocol.TYPE, registry.getType(PrimitiveProtocol.Type.class, "multi-raft"));
    assertEquals(RaftPartitionGroup.TYPE, registry.getType(PartitionGroup.Type.class, "raft"));
  }
}
