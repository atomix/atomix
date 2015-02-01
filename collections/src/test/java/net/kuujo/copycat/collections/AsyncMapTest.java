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

import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.test.ResourceTest;
import net.kuujo.copycat.test.TestCluster;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class AsyncMapTest extends ResourceTest<AsyncMap<String, String>> {

  @Override
  protected TestCluster<AsyncMap<String, String>> createCluster() {
    return TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig().withLog(new BufferedLog())));
  }

  /**
   * Tests putting a value in an asynchronous map and then reading the value.
   */
  public void testAsyncMapPutGet() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig().withLog(new BufferedLog())));
    expectResume();
    cluster.open().thenRun(this::resume);
    await(5000);
    
    AsyncMap<String, String> map = cluster.activeResources().iterator().next();
    expectResume();
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
  public void testAsyncMapPutRemove() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig().withLog(new BufferedLog())));
    expectResume();
    cluster.open().thenRun(this::resume);
    await(5000);
    
    AsyncMap<String, String> map = cluster.activeResources().iterator().next();
    expectResume();
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

  /**
   * Tests getting a value from a passive member of the cluster.
   */
  public void testAsyncMapGetFromPassiveMember() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig()
      .withConsistency(Consistency.WEAK)
      .withLog(new BufferedLog())));
    
    expectResume();
    cluster.open().thenRun(this::resume);
    await(5000);
    
    AsyncMap<String, String> activeMap = cluster.activeResources().iterator().next();
    AsyncMap<String, String> passiveMap = cluster.passiveResources().iterator().next();
    expectResume();
    activeMap.put("foo", "Hello world!").thenRun(() -> {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      passiveMap.get("foo").thenAccept(r1 -> {
        threadAssertEquals(r1, "Hello world!");
        resume();
      });
    });
    await(5000);
  }

  /**
   * Tests putting enough entries in the map's log to roll over the log to a new segment.
   */
  public void testAsyncMapPutMany() throws Throwable {
    TestCluster<AsyncMap<String, String>> cluster = TestCluster.of((uri, config) -> AsyncMap.create("test", uri, config, new AsyncMapConfig()
      .withConsistency(Consistency.WEAK)
      .withLog(new BufferedLog()
        .withSegmentSize(1024)
        .withFlushOnWrite(true))));
    
    expectResume();
    cluster.open().thenRun(this::resume);
    await(5000);
    
    AsyncMap<String, String> map = cluster.activeResources().iterator().next();
    expectResume();
    putMany(map).thenRun(this::resume);
    await(5000);
  }

  /**
   * Puts many entries in the map.
   */
  private CompletableFuture<Void> putMany(AsyncMap<String, String> map) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    putMany(0, 10, map, future);
    return future;
  }

  /**
   * Puts many entries in the map recursively.
   */
  private void putMany(int count, int total, AsyncMap<String, String> map, CompletableFuture<Void> future) {
    if (count < total) {
      map.put(UUID.randomUUID().toString(), "Hello world!").whenComplete((result, error) -> {
        if (error == null) {
          putMany(count + 1, total, map, future);
        } else {
          future.completeExceptionally(error);
        }
      });
    } else {
      future.complete(null);
    }
  }

}
