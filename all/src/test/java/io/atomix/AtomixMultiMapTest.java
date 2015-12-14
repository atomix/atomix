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

import io.atomix.collections.DistributedMultiMap;
import org.testng.annotations.Test;

import java.util.function.Function;

/**
 * Atomix multimap test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixMultiMapTest extends AbstractAtomixTest {

  public void testClientMultiMapGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMultiMap(client1, client2, get("test-client-multimap-get", DistributedMultiMap.TYPE));
  }

  public void testClientMultiMapCreate() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMultiMap(client1, client2, create("test-client-multimap-create", DistributedMultiMap.TYPE));
  }

  public void testReplicaMultiMapGet() throws Throwable {
    testMultiMap(replicas.get(0), replicas.get(1), get("test-replica-multimap-get", DistributedMultiMap.TYPE));
  }

  public void testReplicaMultiMapCreate() throws Throwable {
    testMultiMap(replicas.get(0), replicas.get(1), create("test-replica-multimap-create", DistributedMultiMap.TYPE));
  }

  public void testMixMultiMap() throws Throwable {
    Atomix client = createClient();
    testMultiMap(replicas.get(0), client, create("test-multimap", DistributedMultiMap.TYPE));
  }

  /**
   * Tests creating a distributed multi map.
   */
  private void testMultiMap(Atomix client1, Atomix client2, Function<Atomix, DistributedMultiMap<String, String>> factory) throws Throwable {
    DistributedMultiMap<String, String> map1 = factory.apply(client1);
    map1.put("foo", "Hello world!").join();
    map1.put("foo", "Hello world again!").join();
    map1.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(1000);

    DistributedMultiMap<String, String> map2 = factory.apply(client2);
    map2.get("foo").thenAccept(result -> {
      threadAssertTrue(result.contains("Hello world!"));
      threadAssertTrue(result.contains("Hello world again!"));
      resume();
    });
    await(1000);
  }

}
