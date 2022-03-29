// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.core.Atomix;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * ID generator configuration test.
 */
public class IdGeneratorConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    AtomicIdGeneratorConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("atomic-id-generator");
    assertEquals("atomic-id-generator", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
  }
}
