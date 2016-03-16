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
    createReplicas(3, 3, 0);
  }
  
  public void testClientLockGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLock(client1, client2, getResource("test-client-lock-get", DistributedLock.class));
  }

  public void testReplicaLockGet() throws Throwable {
    testLock(createClient(), replicas.get(0), getResource("test-replica-lock-get", DistributedLock.class));
  }

  /**
   * Tests a leader election.
   */
  private void testLock(Atomix client1, Atomix client2, Function<Atomix, DistributedLock> factory) throws Throwable {
    DistributedLock lock1 = factory.apply(client1);
    DistributedLock lock2 = factory.apply(client2);

    lock1.lock().thenRun(this::resume);
    await(5000);

    lock2.lock().thenRun(this::resume);
    client1.close();
    await(10000);
  }

}
