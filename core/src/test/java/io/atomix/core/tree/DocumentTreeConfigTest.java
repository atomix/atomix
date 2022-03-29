// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree;

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
 * Document tree configuration test.
 */
public class DocumentTreeConfigTest {
  @Test
  public void testLoadConfig() throws Exception {
    AtomicDocumentTreeConfig config = Atomix.config(getClass().getClassLoader().getResource("primitives.conf").getPath())
        .getPrimitive("atomic-document-tree");
    assertEquals("atomic-document-tree", config.getName());
    assertEquals(MultiPrimaryProtocol.TYPE, config.getProtocolConfig().getType());
    assertFalse(config.isReadOnly());
    assertTrue(config.getNamespaceConfig().isRegistrationRequired());
    assertSame(Type1.class, config.getNodeType());
    assertSame(Type2.class, config.getExtraTypes().get(0));
    assertSame(Type3.class, config.getExtraTypes().get(1));
    assertSame(Type1.class, config.getNamespaceConfig().getTypes().get(0).getType());
    assertSame(Type2.class, config.getNamespaceConfig().getTypes().get(1).getType());
  }
}
