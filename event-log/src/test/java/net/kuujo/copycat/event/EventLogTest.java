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
package net.kuujo.copycat.event;

import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.test.ResourceTest;
import net.kuujo.copycat.test.TestCluster;
import org.testng.annotations.Test;

/**
 * Event log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class EventLogTest extends ResourceTest<EventLog<String>> {

  @Override
  protected TestCluster<EventLog<String>> createCluster() {
    return TestCluster.of((uri, config) -> EventLog.create("test", uri, config, new EventLogConfig().withLog(new BufferedLog())));
  }

  /**
   * Tests that a passive member receives events for the event log.
   */
  public void testPassiveEvents() throws Throwable {
    TestCluster<EventLog<String>> cluster = TestCluster.of((uri, config) -> EventLog.create("test", uri, config, new EventLogConfig().withLog(new BufferedLog())));
    expectResume();
    cluster.open().thenRun(this::resume);
    await(15000);

    expectResume();
    EventLog<String> passive = cluster.passiveResources().iterator().next();
    passive.consumer(message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
    });

    EventLog<String> active = cluster.activeResources().iterator().next();
    active.commit("Hello world!");

    await(5000);
  }

}
