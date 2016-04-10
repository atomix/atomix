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

import io.atomix.variables.DistributedLong;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomix long test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixLongTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3);
  }
  
  public void testClientLongGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLong(client1, client2, getResource("test-client-long-get", DistributedLong.class));
  }

  public void testReplicaLongGet() throws Throwable {
    testLong(replicas.get(0), replicas.get(1), getResource("test-replica-long-get", DistributedLong.class));
  }

  /**
   * Tests creating a distributed long.
   */
  private void testLong(Atomix client1, Atomix client2, Function<Atomix, DistributedLong> factory) throws Throwable {
    DistributedLong value1 = factory.apply(client1);
    value1.set(10L).get(5, TimeUnit.SECONDS);
    value1.getAndIncrement().thenAccept(result -> {
      threadAssertEquals(result, 10L);
      resume();
    });
    await(5000);

    DistributedLong value2 = factory.apply(client2);
    value2.incrementAndGet().thenAccept(result -> {
      threadAssertEquals(result, 12L);
      resume();
    });
    await(5000);
  }

}
