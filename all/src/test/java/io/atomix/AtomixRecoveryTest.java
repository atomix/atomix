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
import io.atomix.collections.DistributedSet;
import io.atomix.testing.AbstractAtomixTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * Atomix resource recovery test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class AtomixRecoveryTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
     createReplicas(5);
  }
  
  public void testRecoverClientResources() throws Throwable {
    testRecoverResources(createClient());
  }

  public void testRecoverReplicaResources() throws Throwable {
    testRecoverResources(replicas.get(0));
  }

  /**
   * Tests recovering resources.
   */
  private void testRecoverResources(Atomix atomix) throws Throwable {
    String id = UUID.randomUUID().toString();

    DistributedMap<String, String> map = atomix.create("test-map-" + id, DistributedMap.TYPE).get();
    map.put("foo", "Hello world!").join();
    map.put("bar", "Hello world again!").join();
    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);

    DistributedSet<String> set = atomix.get("test-set-" + id, DistributedSet.TYPE).get();
    set.add("Hello world!").join();

    atomix.client.recover().whenComplete((result, error) -> {
      threadAssertNull(error);
      resume();
    });
    await(10000);

    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);

    set.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);
  }

}
