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

import io.atomix.coordination.DistributedMembershipGroup;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.function.Function;

/**
 * Atomix membership group test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixMembershipGroupTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
     createReplicas(5);
  }
  
  public void testClientMembershipGroupGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMembershipGroup(client1, client2, get("test-client-group-get", DistributedMembershipGroup.class));
  }

  public void testClientMembershipGroupCreate() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testMembershipGroup(client1, client2, create("test-client-group-create", DistributedMembershipGroup.class));
  }

  public void testReplicaMembershipGroupGet() throws Throwable {
    testMembershipGroup(replicas.get(0), replicas.get(1), get("test-replica-group-get", DistributedMembershipGroup.class));
  }

  public void testReplicaMembershipGroupCreate() throws Throwable {
    testMembershipGroup(replicas.get(0), replicas.get(1), create("test-replica-group-create", DistributedMembershipGroup.class));
  }

  public void testMixMembershipGroup() throws Throwable {
    Atomix client = createClient();
    testMembershipGroup(replicas.get(0), client, create("test-group-mix", DistributedMembershipGroup.class));
  }

  /**
   * Tests a membership group.
   */
  private void testMembershipGroup(Atomix client1, Atomix client2, Function<Atomix, DistributedMembershipGroup> factory) throws Throwable {
    DistributedMembershipGroup group1 = factory.apply(client1);
    DistributedMembershipGroup group2 = factory.apply(client2);

    group2.join().thenRun(() -> {
      group2.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 1);
        resume();
      });
    });

    await(5000);

    group1.join().thenRun(() -> {
      group1.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 2);
        resume();
      });
      group2.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 2);
        resume();
      });
    });

    await(5000, 2);

    group1.onLeave(member -> resume());
    group2.leave().thenRun(this::resume);

    await(5000, 2);
  }

}
