// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.core.Atomix;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Distributed counter configuration test.
 */
public class AtomicCounterConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    AtomicCounterConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("atomic-counter");
    assertEquals("atomic-counter", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
  }
}
