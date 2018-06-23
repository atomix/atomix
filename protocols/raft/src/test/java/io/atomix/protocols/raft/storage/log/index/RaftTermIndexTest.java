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
package io.atomix.protocols.raft.storage.log.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Raft term index test.
 */
public class RaftTermIndexTest {
  @Test
  public void testTermIndex() {
    RaftTermIndex index = new RaftTermIndex();
    assertEquals(0, index.lookup(1));
    index.index(1, 1);
    assertEquals(1, index.lookup(1));
    index.index(2, 10);
    index.index(3, 99);
    assertEquals(10, index.lookup(2));
    assertEquals(99, index.lookup(3));
    index.truncate(100);
    assertEquals(99, index.lookup(3));
    index.truncate(99);
    assertEquals(99, index.lookup(3));
    index.truncate(80);
    assertEquals(0, index.lookup(3));
    assertEquals(10, index.lookup(2));
    index.compact(1);
    assertEquals(1, index.lookup(1));
    index.compact(3);
    assertEquals(3, index.lookup(1));
    index.compact(10);
    assertEquals(0, index.lookup(1));
    assertEquals(10, index.lookup(2));
    index.compact(15);
    assertEquals(15, index.lookup(2));
  }
}
