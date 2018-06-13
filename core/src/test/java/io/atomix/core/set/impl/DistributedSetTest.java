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
package io.atomix.core.set.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.set.DistributedSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Distributed set test.
 */
public abstract class DistributedSetTest extends AbstractPrimitiveTest {
  @Test
  public void testSetOperations() throws Exception {
    DistributedSet<String> set = atomix().<String>setBuilder("test-set")
        .withProtocol(protocol())
        .build();

    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    assertFalse(set.contains("foo"));
    assertTrue(set.add("foo"));
    assertTrue(set.contains("foo"));
    assertFalse(set.add("foo"));
    assertTrue(set.contains("foo"));
    assertEquals(1, set.size());
    assertFalse(set.isEmpty());
    assertTrue(set.remove("foo"));
    assertEquals(0, set.size());
    assertFalse(set.contains("foo"));
    assertTrue(set.isEmpty());
    assertFalse(set.remove("foo"));
    assertTrue(set.isEmpty());
  }
}
