// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Transactional map test.
 */
public class TransactionalMapTest extends AbstractPrimitiveTest {
  @Test
  public void testTransactionalMap() throws Throwable {
    Transaction transaction1 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction1.begin();

    TransactionalMap<String, String> map1 = transaction1.<String, String>mapBuilder("test-map")
        .withProtocol(protocol())
        .build();

    Transaction transaction2 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction2.begin();

    TransactionalMap<String, String> map2 = transaction2.<String, String>mapBuilder("test-map")
        .withProtocol(protocol())
        .build();

    try {
      assertNull(map1.get("foo"));
      map1.put("foo", "bar");
      assertEquals("bar", map1.get("foo"));
    } finally {
      assertEquals(CommitStatus.SUCCESS, transaction1.commit());
    }

    try {
      assertEquals("bar", map2.get("foo"));
    } finally {
      assertEquals(CommitStatus.SUCCESS, transaction2.commit());
    }
  }
}
