/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.value.AsyncAtomicValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public class AtomicValueTest extends AbstractPrimitiveTest {
  @Test
  public void testValue() throws Exception {
    AsyncAtomicValue<String> value = atomix().<String>atomicValueBuilder("test-value").build().async();
    assertNull(value.get().join());
    value.set("a").join();
    assertEquals("a", value.get().join());
    assertFalse(value.compareAndSet("b", "c").join());
    assertTrue(value.compareAndSet("a", "b").join());
    assertEquals("b", value.get().join());
    assertEquals("b", value.getAndSet("c").join());
    assertEquals("c", value.get().join());
  }
}
