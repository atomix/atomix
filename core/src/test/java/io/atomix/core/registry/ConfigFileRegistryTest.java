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
 * Config file based registry test.
 */
public class ConfigFileRegistryTest {
  @Test
  public void testConfigFileRegistryFile() throws Exception {
    AtomixRegistry registry = ConfigFileRegistry.builder().withConfigFiles("src/test/resources/registry.conf").build();
    assertEquals(ConsensusProfile.TYPE.name(), registry.getType(Profile.Type.class, "consensus").name());
    assertEquals(BootstrapDiscoveryProvider.TYPE.name(), registry.getType(NodeDiscoveryProvider.Type.class, "bootstrap").name());
    assertEquals(AtomicCounterType.instance().name(), registry.getType(PrimitiveType.class, "atomic-counter").name());
    assertEquals(MultiRaftProtocol.TYPE.name(), registry.getType(PrimitiveProtocol.Type.class, "multi-raft").name());
    assertEquals(RaftPartitionGroup.TYPE.name(), registry.getType(PartitionGroup.Type.class, "raft").name());
  }

  @Test
  public void testConfigFileRegistryResource() throws Exception {
    AtomixRegistry registry = ConfigFileRegistry.builder().withConfigResources("registry").build();
    assertEquals(ConsensusProfile.TYPE.name(), registry.getType(Profile.Type.class, "consensus").name());
    assertEquals(BootstrapDiscoveryProvider.TYPE.name(), registry.getType(NodeDiscoveryProvider.Type.class, "bootstrap").name());
    assertEquals(AtomicCounterType.instance().name(), registry.getType(PrimitiveType.class, "atomic-counter").name());
    assertEquals(MultiRaftProtocol.TYPE.name(), registry.getType(PrimitiveProtocol.Type.class, "multi-raft").name());
    assertEquals(RaftPartitionGroup.TYPE.name(), registry.getType(PartitionGroup.Type.class, "raft").name());
  }
}
