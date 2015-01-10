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
package net.kuujo.copycat.collections;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.log.BufferedLog;
import org.testng.annotations.Test;

/**
 * Asynchronous map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class AsyncMapTest extends ConcurrentTestCase {

  /**
   * Tests putting a value in an asynchronous map and then reading the value.
   */
  public void testAsyncMapPutGet() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig().withLog(new BufferedLog())));
    cluster.open().thenRun(this::resume);
    await(5000);
    AsyncMap<String, String> map = cluster.resources().get(0);
    map.put("foo", "Hello world!").thenRun(() -> {
      map.get("foo").thenAccept(result -> {
        threadAssertEquals(result, "Hello world!");
        resume();
      });
    });
    await(5000);
  }

  /**
   * Tests putting a value in an asynchronous map and then removing it.
   */
  public void testAsyncMapPutRemote() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig().withLog(new BufferedLog())));
    cluster.open().thenRun(this::resume);
    await(5000);
    AsyncMap<String, String> map = cluster.resources().get(0);
    map.put("foo", "Hello world!").thenRun(() -> {
      map.get("foo").thenAccept(r1 -> {
        threadAssertEquals(r1, "Hello world!");
        map.remove("foo").thenRun(() -> {
          map.get("foo").thenAccept(r2 -> {
            threadAssertNull(r2);
            resume();
          });
        });
      });
    });
    await(5000);
  }

}
