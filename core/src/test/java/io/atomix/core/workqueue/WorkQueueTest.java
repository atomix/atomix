/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.workqueue;

import com.google.common.util.concurrent.Uninterruptibles;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.workqueue.impl.WorkQueueProxy;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link WorkQueueProxy}.
 */
public abstract class WorkQueueTest extends AbstractPrimitiveTest<ProxyProtocol> {
  private static final Duration DEFAULT_PROCESSING_TIME = Duration.ofMillis(100);
  private static final String DEFAULT_PAYLOAD = "hello world";

  @Test
  public void testAdd() throws Throwable {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item = DEFAULT_PAYLOAD;
    queue1.addOne(item).get(30, TimeUnit.SECONDS);

    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String task2 = DEFAULT_PAYLOAD;
    queue2.addOne(task2).get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 2);
    assertEquals(stats.totalInProgress(), 0);
    assertEquals(stats.totalCompleted(), 0);
  }

  @Test
  public void testAddMultiple() throws Throwable {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item1 = DEFAULT_PAYLOAD;
    String item2 = DEFAULT_PAYLOAD;
    queue1.addMultiple(Arrays.asList(item1, item2)).get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 2);
    assertEquals(stats.totalInProgress(), 0);
    assertEquals(stats.totalCompleted(), 0);
  }

  @Test
  public void testTakeAndComplete() throws Throwable {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item1 = DEFAULT_PAYLOAD;
    queue1.addOne(item1).get(30, TimeUnit.SECONDS);

    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    Task<String> removedTask = queue2.take().get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue2.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 0);
    assertEquals(stats.totalInProgress(), 1);
    assertEquals(stats.totalCompleted(), 0);

    assertEquals(removedTask.payload(), item1);
    queue2.complete(Arrays.asList(removedTask.taskId())).get(30, TimeUnit.SECONDS);

    stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 0);
    assertEquals(stats.totalInProgress(), 0);
    assertEquals(stats.totalCompleted(), 1);

    // Another take should return null
    assertNull(queue2.take().get(30, TimeUnit.SECONDS));
  }

  @Test
  public void testUnexpectedClientClose() throws Throwable {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item1 = DEFAULT_PAYLOAD;
    queue1.addOne(item1).get(30, TimeUnit.SECONDS);

    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    queue2.take().get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(0, stats.totalPending());
    assertEquals(1, stats.totalInProgress());
    assertEquals(0, stats.totalCompleted());

    queue2.close().get(30, TimeUnit.SECONDS);

    stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(1, stats.totalPending());
    assertEquals(0, stats.totalInProgress());
    assertEquals(0, stats.totalCompleted());
  }

  @Test
  public void testAutomaticTaskProcessing() throws Throwable {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    Executor executor = Executors.newSingleThreadExecutor();

    CountDownLatch latch1 = new CountDownLatch(1);
    queue1.registerTaskProcessor(s -> latch1.countDown(), 2, executor);

    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item1 = DEFAULT_PAYLOAD;
    queue2.addOne(item1).get(30, TimeUnit.SECONDS);

    assertTrue(Uninterruptibles.awaitUninterruptibly(latch1, 5000, TimeUnit.MILLISECONDS));
    queue1.stopProcessing();

    String item2 = DEFAULT_PAYLOAD;
    String item3 = DEFAULT_PAYLOAD;

    Thread.sleep((int) DEFAULT_PROCESSING_TIME.toMillis());

    queue2.addMultiple(Arrays.asList(item2, item3)).get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(2, stats.totalPending());
    assertEquals(0, stats.totalInProgress());
    assertEquals(1, stats.totalCompleted());

    CountDownLatch latch2 = new CountDownLatch(2);
    queue1.registerTaskProcessor(s -> latch2.countDown(), 2, executor);

    Uninterruptibles.awaitUninterruptibly(latch2, 500, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testDestroy() throws Exception {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item = DEFAULT_PAYLOAD;
    queue1.addOne(item).get(30, TimeUnit.SECONDS);

    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String task2 = DEFAULT_PAYLOAD;
    queue2.addOne(task2).get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 2);
    assertEquals(stats.totalInProgress(), 0);
    assertEquals(stats.totalCompleted(), 0);

    queue2.delete().get(30, TimeUnit.SECONDS);

    stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 0);
    assertEquals(stats.totalInProgress(), 0);
    assertEquals(stats.totalCompleted(), 0);
  }

  @Test
  public void testCompleteAttemptWithIncorrectSession() throws Exception {
    String queueName = UUID.randomUUID().toString();
    AsyncWorkQueue<String> queue1 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();
    String item = DEFAULT_PAYLOAD;
    queue1.addOne(item).get(30, TimeUnit.SECONDS);

    Task<String> task = queue1.take().get(30, TimeUnit.SECONDS);
    String taskId = task.taskId();

    // Create another client and get a handle to the same queue.
    AsyncWorkQueue<String> queue2 = atomix().<String>workQueueBuilder(queueName)
        .withProtocol(protocol())
        .build()
        .async();

    // Attempt completing the task with new client and verify task is not completed
    queue2.complete(taskId).get(30, TimeUnit.SECONDS);

    WorkQueueStats stats = queue1.stats().get(30, TimeUnit.SECONDS);
    assertEquals(stats.totalPending(), 0);
    assertEquals(stats.totalInProgress(), 1);
    assertEquals(stats.totalCompleted(), 0);
  }
}
