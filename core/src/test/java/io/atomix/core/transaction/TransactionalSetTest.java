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
package io.atomix.core.transaction;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.set.DistributedSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Transactional set test.
 */
public class TransactionalSetTest extends AbstractPrimitiveTest {
  @Test
  public void testTransactionalSet() throws Throwable {
    Transaction transaction1 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction1.begin();

    TransactionalSet<String> set1 = transaction1.<String>setBuilder("test-transactional-set")
        .withProtocol(protocol())
        .build();

    Transaction transaction2 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction2.begin();

    TransactionalSet<String> set2 = transaction2.<String>setBuilder("test-transactional-set")
        .withProtocol(protocol())
        .build();

    try {
      assertFalse(set1.contains("foo"));
      set1.add("foo");
      assertTrue(set1.contains("foo"));
    } finally {
      assertEquals(CommitStatus.SUCCESS, transaction1.commit());
    }

    try {
      assertTrue(set2.contains("foo"));
      assertFalse(set2.contains("bar"));
      assertTrue(set2.remove("foo"));
      assertFalse(set2.contains("foo"));
      assertTrue(set2.add("bar"));
      assertTrue(set2.contains("bar"));
    } finally {
      assertEquals(CommitStatus.SUCCESS, transaction2.commit());
    }

    DistributedSet<String> set = atomix().<String>setBuilder("test-transactional-set")
        .withProtocol(protocol())
        .build();

    assertFalse(set.isEmpty());
    assertTrue(set.contains("bar"));
    assertEquals(1, set.size());

    Transaction transaction3 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction3.begin();

    TransactionalSet<String> set3 = transaction3.<String>setBuilder("test-transactional-set")
        .withProtocol(protocol())
        .build();

    Transaction transaction4 = atomix().transactionBuilder()
        .withIsolation(Isolation.REPEATABLE_READS)
        .build();
    transaction4.begin();

    TransactionalSet<String> set4 = transaction4.<String>setBuilder("test-transactional-set")
        .withProtocol(protocol())
        .build();

    assertTrue(set3.add("foo"));
    assertTrue(set4.add("foo"));
    assertTrue(set3.remove("bar"));
    assertFalse(set4.add("bar"));

    assertEquals(CommitStatus.SUCCESS, transaction3.commit());
    assertEquals(CommitStatus.FAILURE, transaction4.commit());
  }
}
