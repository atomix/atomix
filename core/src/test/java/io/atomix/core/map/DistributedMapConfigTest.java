// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.core.Atomix;
import io.atomix.core.types.Type1;
import io.atomix.core.types.Type2;
import io.atomix.core.types.Type3;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Distributed map configuration test.
 */
public class DistributedMapConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    DistributedMapConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("map");
    assertEquals("map", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
    assertTrue(config.getNamespaceConfig().isRegistrationRequired());
    assertSame(Type1.class, config.getKeyType());
    assertSame(Type2.class, config.getValueType());
    assertSame(Type3.class, config.getExtraTypes().get(0));
    assertSame(Type1.class, config.getNamespaceConfig().getTypes().get(0).getType());
    assertSame(Type2.class, config.getNamespaceConfig().getTypes().get(1).getType());
  }
}
