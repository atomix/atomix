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
package net.kuujo.copycat.collections;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.test.TestCluster;
import org.testng.annotations.Test;

/**
 * Asynchronous set tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class AsyncSetTest extends ConcurrentTestCase {

  /**
   * Sets adding and removing an item in the set.
   */
  public void testSetAddRemove() throws Throwable {
    TestCluster<AsyncSet<String>> cluster = TestCluster.<AsyncSet<String>>builder()
      .withActiveMembers(3)
      .withPassiveMembers(2)
      .withUriFactory(id -> String.format("local://test%d", id))
      .withClusterFactory(members -> new ClusterConfig().withProtocol(new LocalProtocol()).withMembers(members))
      .withResourceFactory(config -> AsyncSet.create(new AsyncSetConfig(), config))
      .build();

    expectResume();
    cluster.open().thenRun(this::resume);
    await(5000);

    AsyncSet<String> set = cluster.activeResources().iterator().next();
    expectResume();
    set.add("foo").thenRun(() -> {
      set.contains("foo").thenAccept(addContains -> {
        threadAssertTrue(addContains);
        set.remove("foo").thenAccept(removed -> {
          threadAssertTrue(removed);
          set.contains("foo").thenAccept(removeContains -> {
            threadAssertFalse(removeContains);
            resume();
          });
        });
      });
    });
    await(5000);
  }

}
