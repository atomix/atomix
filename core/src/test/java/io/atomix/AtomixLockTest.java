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

import io.atomix.concurrent.DistributedLock;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomix lock test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixLockTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3);
  }
  
  public void testClientLockGet() throws Throwable {
    AtomixClient client1 = createClient();
    AtomixClient client2 = createClient();

    DistributedLock lock1 = client1.getLock("test-client-lock").get(10, TimeUnit.SECONDS);
    lock1.lock().thenRun(this::resume);
    await(5000);

    DistributedLock lock2 = client2.getLock("test-client-lock").get(10, TimeUnit.SECONDS);
    lock2.lock().thenRun(this::resume);
    client1.close();
    await(10000);
  }

  public void testReplicaLockGet() throws Throwable {
    AtomixClient client = createClient();
    AtomixReplica replica = replicas.get(0);

    DistributedLock lock1 = client.getLock("test-replica-lock").get(10, TimeUnit.SECONDS);
    lock1.lock().thenRun(this::resume);
    await(5000);

    DistributedLock lock2 = replica.getLock("test-replica-lock").get(10, TimeUnit.SECONDS);
    lock2.lock().thenRun(this::resume);
    client.close();
    await(10000);
  }

  /**
   * Tests a leader election.
   */
  private void testLock(AtomixClient client1, AtomixClient client2, Function<Atomix, DistributedLock> factory) throws Throwable {
    DistributedLock lock1 = factory.apply(client1);
    DistributedLock lock2 = factory.apply(client2);

    lock1.lock().thenRun(this::resume);
    await(5000);

    lock2.lock().thenRun(this::resume);
    client1.close();
    await(10000);
  }

}
