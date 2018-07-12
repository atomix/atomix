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
import io.atomix.core.profile.Profile;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;
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
    assertFalse(registry.getTypes(PrimitiveType.class).isEmpty());
    assertFalse(registry.getTypes(PrimitiveType.class).isEmpty());
    assertEquals("atomic-map", registry.getType(PrimitiveType.class, "atomic-map").name());
    assertFalse(registry.getTypes(PartitionGroup.Type.class).isEmpty());
    assertEquals("raft", registry.getType(PartitionGroup.Type.class, "raft").name());
    assertFalse(registry.getTypes(PrimitiveProtocol.Type.class).isEmpty());
    assertEquals("multi-raft", registry.getType(PrimitiveProtocol.Type.class, "multi-raft").name());
    assertEquals(3, registry.getTypes(Profile.Type.class).size());
    assertEquals("client", registry.getType(Profile.Type.class, "client").name());
  }
}
