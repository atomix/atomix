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
package io.atomix.collections;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import io.atomix.atomix.testing.AbstractAtomixTest;
import io.atomix.collections.state.SetState;
import io.atomix.resource.ResourceStateMachine;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedSetTest extends AbstractAtomixTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new SetState();
  }

  /**
   * Tests adding and removing members from a set.
   */
  public void testSetAddRemove() throws Throwable {
    createServers(3);

    DistributedSet<String> set1 = new DistributedSet<>(createClient());
    assertFalse(set1.contains("Hello world!").get());

    DistributedSet<String> set2 = new DistributedSet<>(createClient());
    assertFalse(set2.contains("Hello world!").get());

    set1.add("Hello world!").join();
    assertTrue(set1.contains("Hello world!").get());
    assertTrue(set2.contains("Hello world!").get());

    set2.remove("Hello world!").join();
    assertFalse(set1.contains("Hello world!").get());
    assertFalse(set2.contains("Hello world!").get());
  }

}
