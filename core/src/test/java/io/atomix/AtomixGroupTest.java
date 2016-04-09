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

import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalGroupMember;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

/**
 * Atomix membership group test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixGroupTest extends AbstractAtomixTest {
  @BeforeClass
  protected void setupCluster() throws Throwable {
    createReplicas(3);
  }
  
  public void testClientGroupGet() throws Throwable {
    Atomix client1 = createClient();
    Atomix client2 = createClient();
    testGroup(client1, client2, getResource("test-client-group-get", DistributedGroup.class));
  }

  public void testReplicaGroupGet() throws Throwable {
    testGroup(replicas.get(0), replicas.get(1), getResource("test-replica-group-get", DistributedGroup.class));
  }

  /**
   * Tests a membership group.
   */
  private void testGroup(Atomix client1, Atomix client2, Function<Atomix, DistributedGroup> factory) throws Throwable {
    DistributedGroup group1 = factory.apply(client1);
    DistributedGroup group2 = factory.apply(client2);

    LocalGroupMember localMember = group2.join().get(5, TimeUnit.SECONDS);
    assertEquals(group2.members().size(), 1);

    group1.join().thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
      resume();
    });

    await(5000);

    group1.onLeave(member -> resume());
    localMember.leave().thenRun(this::resume);

    await(5000, 2);
  }

}
