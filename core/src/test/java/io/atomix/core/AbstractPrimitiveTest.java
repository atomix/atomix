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
package io.atomix.core;

import io.atomix.cluster.Node;
import io.atomix.primitive.PrimitiveProtocol;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base Atomix test.
 */
public abstract class AbstractPrimitiveTest extends AbstractAtomixTest {
  private static List<Atomix> instances;
  private static int id = 10;

  /**
   * Returns the primitive protocol with which to test.
   *
   * @return the protocol with which to test
   */
  protected abstract PrimitiveProtocol protocol();

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  protected Atomix atomix() throws Exception {
    Atomix instance = createAtomix(Node.Type.CLIENT, id++, Arrays.asList(1, 2, 3), Arrays.asList()).start().get(10, TimeUnit.SECONDS);
    instances.add(instance);
    return instance;
  }

  @BeforeClass
  public static void setupAtomix() throws Exception {
    AbstractAtomixTest.setupAtomix();
    instances = new ArrayList<>();
    instances.add(createAtomix(Node.Type.CORE, 1, Arrays.asList(1, 2, 3), Arrays.asList()));
    instances.add(createAtomix(Node.Type.CORE, 2, Arrays.asList(1, 2, 3), Arrays.asList()));
    instances.add(createAtomix(Node.Type.CORE, 3, Arrays.asList(1, 2, 3), Arrays.asList()));
    List<CompletableFuture<Atomix>> futures = instances.stream().map(Atomix::start).collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void teardownAtomix() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    } catch (Exception e) {
      // Do nothing
    }
    AbstractAtomixTest.teardownAtomix();
  }
}
