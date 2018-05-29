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
package io.atomix.core.utils.config;

import io.atomix.core.AtomixConfig;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.utils.config.ConfigMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Default configuration provider test.
 */
public class ConfigMapperTest {

  private ConfigMapper getMapper() {
    return new ConfigMapper(
        Thread.currentThread().getContextClassLoader(),
        new PartitionGroupConfigMapper(),
        new PrimitiveConfigMapper(),
        new PrimitiveProtocolConfigMapper(),
        new ProfileMapper());
  }

  @Test
  public void testDefaults() throws Exception {
    ConfigMapper mapper = getMapper();
    AtomixConfig config = mapper.loadResources(AtomixConfig.class, "defaults");
    assertEquals("raft", config.getRegistry().getPartitionGroupTypes().get("raft").name());
    assertEquals("primary-backup", config.getRegistry().getPartitionGroupTypes().get("primary-backup").name());
    assertEquals("multi-raft", config.getRegistry().getProtocolTypes().get("multi-raft").name());
    assertEquals("multi-primary", config.getRegistry().getProtocolTypes().get("multi-primary").name());
  }

  @Test
  public void testOverrides() throws Exception {
    ConfigMapper mapper = getMapper();
    AtomixConfig config = mapper.loadResources(AtomixConfig.class, "test", "defaults");
    assertEquals("raft", config.getRegistry().getPartitionGroupTypes().get("raft").name());
    assertEquals("primary-backup", config.getRegistry().getPartitionGroupTypes().get("primary-backup").name());
    assertEquals("multi-raft", config.getRegistry().getProtocolTypes().get("multi-raft").name());
    assertEquals("multi-primary", config.getRegistry().getProtocolTypes().get("multi-primary").name());
    assertEquals(3, config.getClusterConfig().getMembers().size());
    assertEquals("raft", config.getManagementGroup().getType());
    assertEquals(1, config.getManagementGroup().getPartitions());
    assertEquals("raft", config.getPartitionGroups().get("one").getType());
    assertEquals(7, config.getPartitionGroups().get("one").getPartitions());
    assertEquals("primary-backup", config.getPartitionGroups().get("two").getType());
    assertEquals(32, config.getPartitionGroups().get("two").getPartitions());
    assertEquals(2, config.getProfiles().size());
    assertEquals(1024 * 1024 * 16, ((RaftPartitionGroupConfig) config.getManagementGroup()).getSegmentSize().bytes());
  }
}
