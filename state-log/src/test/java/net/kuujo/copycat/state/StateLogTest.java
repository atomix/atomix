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
package net.kuujo.copycat.state;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.LocalProtocol;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

/**
 * State log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class StateLogTest extends ConcurrentTestCase {

  /**
   * Test test.
   */
  @SuppressWarnings("unchecked")
  public void testQueryWithStrongConsistency() throws Throwable {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol())
      .withMembers("local://foo", "local://bar", "local://baz");
    StateLog<String> log1 = StateLog.<String>create("test", cluster.copy().withLocalMember("local://foo"), new StateLogConfig().withLog(new BufferedLog()).withDefaultConsistency(Consistency.STRONG)).registerQuery("test", v -> v);
    StateLog<String> log2 = StateLog.<String>create("test", cluster.copy().withLocalMember("local://bar"), new StateLogConfig().withLog(new BufferedLog()).withDefaultConsistency(Consistency.STRONG)).registerQuery("test", v -> v);
    StateLog<String> log3 = StateLog.<String>create("test", cluster.copy().withLocalMember("local://baz"), new StateLogConfig().withLog(new BufferedLog()).withDefaultConsistency(Consistency.STRONG)).registerQuery("test", v -> v);

    CompletableFuture<StateLog<String>>[] futures = new CompletableFuture[3];
    futures[0] = log1.open();
    futures[1] = log2.open();
    futures[2] = log3.open();

    expectResume();
    CompletableFuture.allOf(futures).thenRun(this::resume);
    await(15000);

    expectResume();
    log1.submit("test", "Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(5000);
  }

}
