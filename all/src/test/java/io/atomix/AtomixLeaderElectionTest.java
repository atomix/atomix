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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.atomix.testing.AbstractAtomixTest;
import io.atomix.coordination.DistributedLeaderElection;

/**
 * Atomix leader election test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixLeaderElectionTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
     createReplicas(5);
  }
  
  public void testClientLeaderElectionGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLeaderElection(client1, client2, get("test-client-election-get", DistributedLeaderElection.TYPE));
  }

  public void testClientLeaderElectionCreate() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testLeaderElection(client1, client2, create("test-client-election-create", DistributedLeaderElection.TYPE));
  }

  public void testReplicaLeaderElectionGet() throws Throwable {
    testLeaderElection(createClient(), replicas.get(1), get("test-replica-election-get", DistributedLeaderElection.TYPE));
  }

  public void testReplicaLeaderElectionCreate() throws Throwable {
    testLeaderElection(createClient(), replicas.get(1), create("test-replica-election-create", DistributedLeaderElection.TYPE));
  }

  /**
   * Tests a leader election.
   */
  private void testLeaderElection(Atomix client1, Atomix client2, Function<Atomix, DistributedLeaderElection> factory) throws Throwable {
    DistributedLeaderElection election1 = factory.apply(client1);
    DistributedLeaderElection election2 = factory.apply(client2);

    AtomicLong lastEpoch = new AtomicLong(0);
    election1.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    await(10000);

    election2.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    client1.close();

    await(10000);
  }

}
