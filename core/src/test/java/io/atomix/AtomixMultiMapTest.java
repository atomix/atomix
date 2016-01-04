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

import java.util.function.Function;

import io.atomix.manager.ResourceManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.atomix.testing.AbstractAtomixTest;
import io.atomix.collections.DistributedMultiMap;

/**
 * Atomix multimap test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixMultiMapTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
     createReplicas(5);
  }
  
  public void testClientMultiMapGet() throws Throwable {
    ResourceManager client1 = createClient();
    ResourceManager client2 = createClient();
    testMultiMap(client1, client2, get("test-client-multimap-get", DistributedMultiMap.class));
  }

  public void testClientMultiMapCreate() throws Throwable {
    ResourceManager client1 = createClient();
    ResourceManager client2 = createClient();
    testMultiMap(client1, client2, create("test-client-multimap-create", DistributedMultiMap.class));
  }

  public void testReplicaMultiMapGet() throws Throwable {
    testMultiMap(replicas.get(0), replicas.get(1), get("test-replica-multimap-get", DistributedMultiMap.class));
  }

  public void testReplicaMultiMapCreate() throws Throwable {
    testMultiMap(replicas.get(0), replicas.get(1), create("test-replica-multimap-create", DistributedMultiMap.class));
  }

  public void testMixMultiMap() throws Throwable {
    ResourceManager client = createClient();
    testMultiMap(replicas.get(0), client, create("test-multimap", DistributedMultiMap.class));
  }

  /**
   * Tests creating a distributed multi map.
   */
  private void testMultiMap(ResourceManager client1, ResourceManager client2, Function<ResourceManager, DistributedMultiMap<String, String>> factory) throws Throwable {
    DistributedMultiMap<String, String> map1 = factory.apply(client1);
    map1.put("foo", "Hello world!").join();
    map1.put("foo", "Hello world again!").join();
    map1.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(5000);

    DistributedMultiMap<String, String> map2 = factory.apply(client2);
    map2.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(5000);
  }

}
