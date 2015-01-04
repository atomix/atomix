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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.AsyncMap;
import net.kuujo.copycat.collections.AsyncMapConfig;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.protocol.LocalProtocol;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.*;

/**
 * Copycat tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CopycatTest {

  /**
   * Tests the Copycat instance.
   */
  public void testCopycat() throws Exception {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol())
      .withMembers("local://foo", "local://bar", "local://baz");

    CopycatConfig config = new CopycatConfig()
      .withClusterConfig(cluster)
      .addMapConfig("test", new AsyncMapConfig());

    Copycat copycat1 = Copycat.create("local://foo", config);
    Copycat copycat2 = Copycat.create("local://bar", config);
    Copycat copycat3 = Copycat.create("local://baz", config);

    CountDownLatch latch = new CountDownLatch(3);

    copycat1.open().thenRun(latch::countDown);
    copycat2.open().thenRun(latch::countDown);
    copycat3.open().thenRun(latch::countDown);

    latch.await(30, TimeUnit.SECONDS);

    AsyncMap<String, String> map = copycat1.map("test");
    Assert.assertTrue(map.isEmpty().get());
    map.put("foo", "Hello world!").get();
    Assert.assertFalse(map.isEmpty().get());
    Assert.assertEquals(map.size().get(), Integer.valueOf(1));
    Assert.assertEquals(map.get("foo").get(), "Hello world!");
    map.clear().get();
    Assert.assertEquals(map.size().get(), Integer.valueOf(0));
  }

  public void testThreads() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture.supplyAsync(() -> {
      System.out.println(Thread.currentThread().getName());
      return "Hello world!";
    }, testExecutor1)
      .thenApplyAsync(string -> {
        System.out.println(Thread.currentThread().getName());
        return string;
      }, testExecutor1)
      .thenRunAsync(() -> {
        System.out.println(Thread.currentThread().getName());
      }, testExecutor2)
      .thenRunAsync(() -> {
        System.out.println(Thread.currentThread().getName());
      }, testExecutor2)
      .thenComposeAsync(result -> {
        System.out.println("ASYNC " + Thread.currentThread().getName());
        return CompletableFuture.supplyAsync(() -> {
          System.out.println("ASYNC " + Thread.currentThread().getName());
          return "foo";
        }, testExecutor2);
      }, testExecutor1);

    System.out.println("YES");

    while(true);
  }

  public void testThreads2() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void> future1 = new CompletableFuture<>();
    testExecutor1.execute(() -> {
      System.out.println(Thread.currentThread().getName());
      future1.complete(null);
    });

    future1.thenRunAsync(() -> {
      System.out.println(Thread.currentThread().getName());
    }, testExecutor2);

    System.out.println("YES");

    while(true);
  }

  public void testThreads3() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread()
      .getName()), testExecutor1);
    future2.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);

    System.out.println("NO");

    while(true);
  }

  public void testThreads4() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    testExecutor1.execute(() -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      testExecutor2.execute(() -> {
        System.out.println(Thread.currentThread().getName());
        testExecutor1.execute(() -> future.complete(null));
      });
      future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    });

    while(true);
  }

  public void testThreads5() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void>[] futures = new CompletableFuture[3];
    futures[0] = CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    futures[1] = CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    futures[2] = CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);

    CompletableFuture.allOf(futures).thenRun(() -> System.out.println(Thread.currentThread().getName()));

    while(true);
  }

  public void testThreads6() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);

    System.out.println("YES");

    while(true);
  }

  public void testThreads7() throws Exception {
    System.out.println(Thread.currentThread().getName());

    CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()))
      .thenRun(() -> System.out.println(Thread.currentThread().getName()));

    System.out.println("YES");

    while(true);
  }

  public void testThreads8() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void> future = new CompletableFuture<>();
    testExecutor1.execute(() -> {
      System.out.println(Thread.currentThread().getName());
      future.complete(null);
    });

    future.thenRun(() -> System.out.println(Thread.currentThread().getName()));

    System.out.println("YES");

    while(true);
  }

  public void testThreads9() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
      System.out.println(Thread.currentThread().getName());
      return "Hello world!";
    }, testExecutor1);

    System.out.println("YES");

    future.thenAccept(foo -> System.out.println(Thread.currentThread().getName() + " " + foo));

    System.out.println("NO");

    while(true);
  }

  public void testThreads10() throws Exception {
    ExecutorService testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void> future = testExecutor1.submit(() -> {
      return CompletableFuture.runAsync(() -> {
        System.out.println(Thread.currentThread().getName());
      }, testExecutor2);
    }).get();

    System.out.println("YES");

    future.thenRun(() -> System.out.println(Thread.currentThread().getName()));

    System.out.println("NO");

    while(true);
  }

  public void testThreads11() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture.completedFuture(null)
      .thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1)
      .thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);

    System.out.println("YES");

    while(true);
  }

  public void testThreads13() throws Exception {
    Executor testExecutor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor testExecutor2 = Executors.newSingleThreadExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor1);
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), testExecutor2);

    System.out.println("YES");

    future.thenRun(() -> System.out.println(Thread.currentThread().getName()));

    System.out.println("NO");

    while(true);
  }

  public void testThreads14() throws Exception {
    Executor executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    System.out.println(Thread.currentThread().getId());

    CompletableFuture<Void> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      System.out.println(Thread.currentThread().getName());
      future.complete(null);
    });
    future.thenRunAsync(() -> System.out.println(Thread.currentThread().getName()), executor);
    future.whenComplete((result, error) -> System.out.println(Thread.currentThread().getName()));
    future.thenRun(() -> System.out.println(Thread.currentThread().getName()));
    while (true);
  }

  public void testThreads15() throws Exception {
    Executor executor1 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-1-%d"));
    Executor executor2 = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("test-thread-2-%d"));
    System.out.println(Thread.currentThread().getName());

    CompletableFuture.runAsync(() -> {
      System.out.println(Thread.currentThread().getName());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }, executor1)
      .thenRun(() -> System.out.println(Thread.currentThread().getName()))
      .thenRunAsync(() -> {
        System.out.println(Thread.currentThread().getName());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }, executor2)
      .thenRun(() -> System.out.println(Thread.currentThread().getName()));

    System.out.println(Thread.currentThread().getName());
    while (true);
  }

}
