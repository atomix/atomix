/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License
 */
package io.atomix;

import io.atomix.collections.DistributedMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomix map test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixMapTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3, 3, 0);
  }
  
  public void testClientMapGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMap(client1, client2, getResource("test-client-map-get", DistributedMap.class));
  }

  public void testReplicaMapGet() throws Throwable {
    testMap(replicas.get(0), replicas.get(1), getResource("test-replica-map-get", DistributedMap.class));
  }

  /**
   * Tests creating a distributed map.
   */
  private void testMap(Atomix client1, Atomix client2, Function<Atomix, DistributedMap<String, String>> factory) throws Throwable {
    DistributedMap<String, String> map1 = factory.apply(client1);
    map1.put("foo", "Hello world!").get(5, TimeUnit.SECONDS);
    map1.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(5000);

    DistributedMap<String, String> map2 = factory.apply(client2);
    map2.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(5000);
  }

}
