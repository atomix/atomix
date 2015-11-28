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
 * limitations under the License.
 */
package io.atomix.coordination;

import io.atomix.coordination.state.LeaderElectionState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ResourceStateMachine;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Async leader election test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedLeaderElectionTest extends AbstractCoordinationTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new LeaderElectionState();
  }

  /**
   * Tests winning leadership.
   */
  public void testElection() throws Throwable {
    createServers(3);

    DistributedLeaderElection election = new DistributedLeaderElection(createClient());

    election.onElection(v -> resume()).thenRun(this::resume);
    await(0, 2);
  }

  /**
   * Tests stepping down leadership.
   */
  public void testNextElection() throws Throwable {
    createServers(3);

    CopycatClient client1 = createClient();
    CopycatClient client2 = createClient();

    DistributedLeaderElection election1 = new DistributedLeaderElection(client1);
    DistributedLeaderElection election2 = new DistributedLeaderElection(client2);

    AtomicLong lastEpoch = new AtomicLong(0);
    election1.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    await();

    election2.onElection(epoch -> {
      threadAssertTrue(epoch > lastEpoch.get());
      lastEpoch.set(epoch);
      resume();
    }).join();

    client1.close();

    await();
  }

}
