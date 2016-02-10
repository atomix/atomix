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

import io.atomix.collections.DistributedSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomix set test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixSetTest extends AbstractAtomixTest {

  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3, 3, 0);
  }
  
  public void testClientSetGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testSet(client1, client2, get("test-client-set-get", DistributedSet.class));
  }

  public void testReplicaSetGet() throws Throwable {
    testSet(replicas.get(0), replicas.get(1), get("test-replica-set-get", DistributedSet.class));
  }

  /**
   * Tests creating a distributed set.
   */
  private void testSet(Atomix client1, Atomix client2, Function<Atomix, DistributedSet<String>> factory) throws Throwable {
    DistributedSet<String> set1 = factory.apply(client1);
    set1.add("Hello world!").get(5, TimeUnit.SECONDS);
    set1.add("Hello world again!").get(5, TimeUnit.SECONDS);
    set1.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);

    DistributedSet<String> set2 = factory.apply(client2);
    set2.contains("Hello world!").thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(1000);
  }

}
