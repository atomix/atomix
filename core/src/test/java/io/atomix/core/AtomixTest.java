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
package io.atomix.core;

import io.atomix.cluster.Node;
import io.atomix.utils.concurrent.Futures;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Atomix test.
 */
public class AtomixTest extends AbstractAtomixTest {
  private List<Atomix> instances;

  @Before
  public void setupInstances() throws Exception {
    AbstractAtomixTest.setupAtomix();
    instances = new ArrayList<>();
  }

  @After
  public void teardownInstances() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    } catch (Exception e) {
      // Do nothing
    }
    AbstractAtomixTest.teardownAtomix();
  }

  protected CompletableFuture<Atomix> startAtomix(int numPartitions, Node.Type type, int id, Integer... ids) {
    Atomix atomix = createAtomix(numPartitions, type, id, ids);
    instances.add(atomix);
    return atomix.start();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUp() throws Exception {
    Atomix atomix1 = startAtomix(3, Node.Type.DATA, 1, 1).join();
    Atomix atomix2 = startAtomix(3, Node.Type.DATA, 2, 1, 2).join();
    Atomix atomix3 = startAtomix(3, Node.Type.DATA, 3, 1, 2, 3).join();
  }

  /**
   * Tests scaling down a cluster.
   */
  @Test
  public void testScaleDown() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(3, Node.Type.DATA, 1, 1, 2, 3));
    futures.add(startAtomix(3, Node.Type.DATA, 2, 1, 2, 3));
    futures.add(startAtomix(3, Node.Type.DATA, 3, 1, 2, 3));
    Futures.allOf(futures).join();
    instances.get(0).stop().join();
    instances.get(1).stop().join();
    instances.get(2).stop().join();
  }
}
