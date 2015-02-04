/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.election;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.LocalProtocol;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Leader election test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LeaderElectionTest {

  /**
   * Tests a leader election with a cluster seed member.
   */
  public void testLeaderElectionAsSeedMember() throws Exception {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol())
      .withMembers("local://foo", "local://bar", "local://baz");

    LeaderElection election1 = LeaderElection.create("test", cluster.copy().withLocalMember("local://foo"));
    LeaderElection election2 = LeaderElection.create("test", cluster.copy().withLocalMember("local://bar"));
    LeaderElection election3 = LeaderElection.create("test", cluster.copy().withLocalMember("local://baz"));

    CountDownLatch latch = new CountDownLatch(3);

    election1.open().thenRun(latch::countDown);
    election2.open().thenRun(latch::countDown);
    election3.open().thenRun(latch::countDown);

    latch.await(30, TimeUnit.SECONDS);
  }

}
