// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

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
 * Distributed set configuration test.
 */
public class DistributedSetConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    DistributedSetConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("set");
    assertEquals("set", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
    assertSame(Type1.class, config.getElementType());
    assertSame(Type3.class, config.getExtraTypes().get(0));
    assertTrue(config.getNamespaceConfig().isRegistrationRequired());
    assertSame(Type1.class, config.getNamespaceConfig().getTypes().get(0).getType());
    assertSame(Type2.class, config.getNamespaceConfig().getTypes().get(1).getType());
  }
}
