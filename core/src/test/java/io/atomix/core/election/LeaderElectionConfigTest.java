// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.core.Atomix;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Leader election configuration test.
 */
public class LeaderElectionConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    LeaderElectionConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("leader-election");
    assertEquals("leader-election", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
  }
}
