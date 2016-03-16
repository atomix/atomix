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

import io.atomix.collections.DistributedQueue;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomix queue test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixQueueTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3, 3, 0);
  }
  
  public void testClientQueueGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testQueue(client1, client2, getResource("test-client-queue-get", DistributedQueue.class));
  }

  public void testReplicaQueueGet() throws Throwable {
    testQueue(replicas.get(0), replicas.get(1), getResource("test-replica-queue-get", DistributedQueue.class));
  }

  /**
   * Tests creating a distributed queue.
   */
  private void testQueue(Atomix client1, Atomix client2, Function<Atomix, DistributedQueue<String>> factory) throws Throwable {
    DistributedQueue<String> queue1 = factory.apply(client1);
    queue1.offer("Hello world!").get(5, TimeUnit.SECONDS);
    queue1.offer("Hello world again!").get(5, TimeUnit.SECONDS);
    queue1.poll().thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(1000);

    DistributedQueue<String> queue2 = factory.apply(client2);
    queue2.poll().thenAccept(result -> {
      threadAssertEquals(result, "Hello world again!");
      resume();
    });
    
    await(10000);
  }

}
