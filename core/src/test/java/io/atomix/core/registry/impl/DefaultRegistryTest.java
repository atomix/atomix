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
package io.atomix.core.registry.impl;

import io.atomix.core.AtomixRegistry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Default registry test.
 */
public class DefaultRegistryTest {
  @Test
  public void testRegistry() throws Exception {
    AtomixRegistry registry = AtomixRegistry.registry();
    assertFalse(registry.primitiveTypes().getPrimitiveTypes().isEmpty());
    assertEquals("consistent-map", registry.primitiveTypes().getPrimitiveType("consistent-map").name());
    assertFalse(registry.partitionGroupTypes().getGroupTypes().isEmpty());
    assertEquals("raft", registry.partitionGroupTypes().getGroupType("raft").name());
    assertFalse(registry.protocolTypes().getProtocolTypes().isEmpty());
    assertEquals("multi-raft", registry.protocolTypes().getProtocolType("multi-raft").name());
    assertEquals(3, registry.profiles().getProfiles().size());
    assertEquals("client", registry.profiles().getProfile("client").name());
  }
}
