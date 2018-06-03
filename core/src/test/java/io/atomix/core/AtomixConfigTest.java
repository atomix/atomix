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
package io.atomix.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Atomix configuration test.
 */
public class AtomixConfigTest {
  @Test
  public void testDefaultAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config();
    assertTrue(config.getClusterConfig().getMembers().isEmpty());
    assertTrue(config.getPartitionGroups().isEmpty());
    assertTrue(config.getProfiles().isEmpty());
  }

  @Test
  public void testAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config("test");
    assertEquals(3, config.getClusterConfig().getMembers().size());
    assertEquals("raft", config.getManagementGroup().getType().name());
    assertEquals(2, config.getPartitionGroups().size());
    assertEquals(2, config.getProfiles().size());
  }
}
