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
package io.atomix.core.barrier;

import io.atomix.core.Atomix;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.raft.MultiRaftProtocolConfig;
import io.atomix.utils.serializer.NamespaceConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Cyclic barrier configuration test.
 */
public class DistributedCyclicBarrierConfigTest {
  @Test
  public void testConfig() throws Exception {
    DistributedCyclicBarrierConfig config = new DistributedCyclicBarrierConfig();
    assertNull(config.getName());
    assertEquals(DistributedCyclicBarrierType.instance(), config.getType());
    assertNull(config.getNamespaceConfig());
    assertNull(config.getProtocolConfig());
    assertFalse(config.isReadOnly());

    config.setName("foo");
    config.setNamespaceConfig(new NamespaceConfig().setName("test").setCompatible(true).setRegistrationRequired(false));
    config.setProtocolConfig(new MultiRaftProtocolConfig().setGroup("test-group"));
    config.setReadOnly(true);

    assertEquals("foo", config.getName());
    assertEquals("test", config.getNamespaceConfig().getName());
    assertEquals("test-group", ((MultiRaftProtocolConfig) config.getProtocolConfig()).getGroup());
    assertTrue(config.isReadOnly());
  }

  @Test
  public void testLoadConfig() throws Exception {
    DistributedCyclicBarrierConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("cyclic-barrier");
    assertEquals("cyclic-barrier", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
  }
}
