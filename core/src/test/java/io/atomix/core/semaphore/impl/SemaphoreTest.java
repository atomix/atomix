/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.Atomix;
import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreServiceConfig;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.Version;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Semaphore test.
 */
public abstract class SemaphoreTest extends AbstractPrimitiveTest {

  @Test
  public void testInit() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore100 = atomix.semaphoreBuilder("test-semaphore-init-100", protocol())
        .withInitialCapacity(100)
        .build()
        .async();
    AsyncDistributedSemaphore semaphore0 = atomix.semaphoreBuilder("test-semaphore-init-0", protocol())
        .build()
        .async();
    assertEquals(100, semaphore100.availablePermits().get().intValue());
    assertEquals(0, semaphore0.availablePermits().get().intValue());

    AsyncDistributedSemaphore semaphoreNoInit = atomix.semaphoreBuilder("test-semaphore-init-100", protocol())
        .build()
        .async();
    assertEquals(100, semaphoreNoInit.availablePermits().get().intValue());
  }

  @Test
  public void testAcquireRelease() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-base", protocol())
        .withInitialCapacity(10)
        .build()
        .async();

    assertEquals(10, semaphore.availablePermits().get().intValue());
    semaphore.acquire().join();
    assertEquals(9, semaphore.availablePermits().get().intValue());
    semaphore.acquire(9).join();
    assertEquals(0, semaphore.availablePermits().get().intValue());

    semaphore.release().join();
    assertEquals(1, semaphore.availablePermits().get().intValue());
    semaphore.release(100).join();
    assertEquals(101, semaphore.availablePermits().get().intValue());
  }

  @Test
  public void testIncreaseReduceDrain() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-ird", protocol())
        .withInitialCapacity(-10)
        .build()
        .async();

    assertEquals(-10, semaphore.availablePermits().get().intValue());
    assertEquals(10, semaphore.increase(20).get().intValue());
    assertEquals(-10, semaphore.reduce(20).get().intValue());
    assertEquals(-10, semaphore.drainPermits().get().intValue());
    assertEquals(0, semaphore.availablePermits().get().intValue());
  }

  @Test
  public void testOverflow() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-overflow", protocol())
        .withInitialCapacity(Integer.MAX_VALUE)
        .build()
        .async();

    assertEquals(Integer.MAX_VALUE, semaphore.increase(10).get().intValue());
    semaphore.release(10).join();
    assertEquals(Integer.MAX_VALUE, semaphore.availablePermits().get().intValue());
    assertEquals(Integer.MAX_VALUE, semaphore.drainPermits().get().intValue());

    semaphore.reduce(Integer.MAX_VALUE).join();
    semaphore.reduce(Integer.MAX_VALUE).join();
    assertEquals(Integer.MIN_VALUE, semaphore.availablePermits().get().intValue());
  }

  @Test(timeout = 10000)
  public void testTimeout() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-timeout", protocol())
        .withInitialCapacity(10)
        .build()
        .async();

    assertFalse(semaphore.tryAcquire(11).join().isPresent());
    assertTrue(semaphore.tryAcquire(10).join().isPresent());

    long start = System.currentTimeMillis();
    assertFalse(semaphore.tryAcquire(Duration.ofSeconds(1)).join().isPresent());
    long end = System.currentTimeMillis();
    assertTrue(end - start >= 1000);
    assertTrue(end - start < 1100);

    semaphore.release().join();
    start = System.currentTimeMillis();
    assertTrue(semaphore.tryAcquire(Duration.ofMillis(100)).join().isPresent());
    end = System.currentTimeMillis();
    assertTrue(end - start < 100);

    CompletableFuture<Optional<Version>> future = semaphore.tryAcquire(Duration.ofSeconds(1));
    semaphore.release().join();
    assertTrue(future.join().isPresent());
  }


  @Test
  public void testReleaseSession() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphoreProxy semaphore =
        (DistributedSemaphoreProxy) (atomix.semaphoreBuilder("test-semaphore-releaseSession", protocol())
            .withInitialCapacity(10)
            .build()
            .async());

    DistributedSemaphoreProxy semaphore2 =
        (DistributedSemaphoreProxy) (atomix.semaphoreBuilder("test-semaphore-releaseSession", protocol())
            .withInitialCapacity(10)
            .build()
            .async());

    assertEquals(10, semaphore2.drainPermits().join().intValue());

    Map<Long, Integer> status = semaphore.holderStatus().get();
    assertEquals(1, status.size());
    status.values().forEach(permits -> assertEquals(10, permits.intValue()));

    semaphore2.close().join();

    Map<Long, Integer> status2 = semaphore.holderStatus().get();
    assertEquals(0, status2.size());
    assertEquals(10, semaphore.availablePermits().get().intValue());

  }

  @Test
  public void testHolderStatus() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphoreProxy semaphore =
        (DistributedSemaphoreProxy) (atomix.semaphoreBuilder("test-semaphore-holders", protocol())
            .withInitialCapacity(10)
            .build()
            .async());

    DistributedSemaphoreProxy semaphore2 =
        (DistributedSemaphoreProxy) (atomix.semaphoreBuilder("test-semaphore-holders", protocol())
            .withInitialCapacity(10)
            .build()
            .async());

    assertEquals(0, semaphore.holderStatus().get().size());

    semaphore.acquire().join();
    semaphore2.acquire().join();

    Map<Long, Integer> status = semaphore.holderStatus().get();
    assertEquals(2, status.size());
    status.values().forEach(permits -> assertEquals(1, permits.intValue()));

    semaphore.acquire().join();
    semaphore2.acquire().join();

    Map<Long, Integer> status2 = semaphore.holderStatus().get();
    assertEquals(2, status2.size());
    status2.values().forEach(permits -> assertEquals(2, permits.intValue()));

    semaphore.release(2).join();
    semaphore2.release(2).join();
    assertEquals(0, semaphore.holderStatus().get().size());
  }

  @Test
  public void testBlocking() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-blocking", protocol())
            .withInitialCapacity(10)
            .build();

    semaphore.acquire();
    assertEquals(9, semaphore.availablePermits());

    semaphore.release();
    assertEquals(10, semaphore.availablePermits());

    semaphore.increase(10);
    assertEquals(20, semaphore.availablePermits());

    semaphore.reduce(10);
    assertEquals(10, semaphore.availablePermits());

    assertFalse(semaphore.tryAcquire(11).isPresent());
    assertFalse(semaphore.tryAcquire(11, Duration.ofMillis(1)).isPresent());
    assertTrue(semaphore.tryAcquire(5).isPresent());

    assertEquals(5, semaphore.drainPermits());

    DistributedSemaphore semaphore2 =
        atomix.semaphoreBuilder("test-semaphore-blocking", protocol())
            .withInitialCapacity(10)
            .build();

    assertEquals(0, semaphore2.availablePermits());
    semaphore.close();
    assertEquals(10, semaphore2.availablePermits());
  }

  @Test(timeout = 10000)
  public void testQueue() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-queue", protocol())
            .withInitialCapacity(10)
            .build()
            .async();

    CompletableFuture<Version> future20 = semaphore.acquire(20);
    CompletableFuture<Version> future11 = semaphore.acquire(11);

    semaphore.increase(1);
    assertNotNull(future11.join());
    assertFalse(future20.isDone());
    assertEquals(0, semaphore.availablePermits().get().intValue());

    CompletableFuture<Version> future21 = semaphore.acquire(21);
    CompletableFuture<Version> future5 = semaphore.acquire(5);

    // wakeup 5 and 20
    semaphore.release(25);
    future5.join();
    future20.join();
    assertFalse(future21.isDone());
  }

  @Test
  public void testExpire() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-expire", protocol())
            .withInitialCapacity(10)
            .build()
            .async();

    assertEquals(10, semaphore.availablePermits().get().intValue());

    CompletableFuture<Optional<Version>> future = semaphore.tryAcquire(11, Duration.ofMillis(500));
    Thread.sleep(500);
    assertFalse(future.join().isPresent());
    assertEquals(10, semaphore.availablePermits().get().intValue());
  }

  @Test(timeout = 10000)
  public void testInterrupt() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-interrupt", protocol())
        .withInitialCapacity(10)
        .build();

    DistributedSemaphore semaphore2 = atomix.semaphoreBuilder("test-semaphore-interrupt", protocol())
        .withInitialCapacity(10)
        .build();

    AtomicBoolean interrupted = new AtomicBoolean();

    Thread t = new Thread(() -> {
      try {
        semaphore.acquire(11);
      } catch (PrimitiveException.Interrupted e) {
        synchronized (interrupted) {
          interrupted.set(true);
          interrupted.notifyAll();
        }
      }
    });
    t.start();
    synchronized (interrupted) {
      t.interrupt();
      interrupted.wait();
    }

    assertTrue(interrupted.get());
    semaphore2.increase(1);

    // wait asynchronous release.
    Thread.sleep(1000);
    assertEquals(11, semaphore.availablePermits());
  }

  @Test(timeout = 30000)
  public void testBlockTimeout() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-block-timeout", protocol())
        .withInitialCapacity(10)
        .build();

    DistributedSemaphore semaphore2 = atomix.semaphoreBuilder("test-semaphore-block-timeout", protocol())
        .withInitialCapacity(10)
        .build();

    Object timedout = new Object();

    Thread t = new Thread(() -> {
      try {
        semaphore.acquire(11);
      } catch (PrimitiveException.Timeout e) {
        synchronized (timedout) {
          timedout.notifyAll();
        }
      }
    });
    t.start();

    synchronized (timedout) {
      timedout.wait();
    }

    semaphore2.increase(1);

    // wait asynchronous release.
    Thread.sleep(1000);
    assertEquals(11, semaphore.availablePermits());
  }


  @Test(timeout = 60000)
  @Ignore("depends on performance")
  public void testExpireRace() throws Exception {
    int TEST_COUNT = 10000;
    int threads = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    Atomix atomix = atomix();

    List<Future<?>> taskFuture = new ArrayList<>(threads);
    AtomicInteger acquired = new AtomicInteger();

    for (int i = 0; i < threads; i++) {
      taskFuture.add(executorService.submit(() -> {
        AsyncDistributedSemaphore semaphore =
            atomix.semaphoreBuilder("test-semaphore-race", protocol())
                .withInitialCapacity(TEST_COUNT)
                .build()
                .async();
        while (acquired.get() < TEST_COUNT) {
          semaphore.tryAcquire(Duration.ofMillis(1)).join().ifPresent(v -> acquired.incrementAndGet());
        }
      }));
    }

    for (Future<?> future : taskFuture) {
      future.get();
    }

    executorService.shutdown();
  }

  @Test
  @Ignore
  public void testSnapshot() throws Exception {
    DefaultDistributedSemaphoreService service = new DefaultDistributedSemaphoreService(
        new DistributedSemaphoreServiceConfig().setInitialCapacity(10));

    Field available = DefaultDistributedSemaphoreService.class.getDeclaredField("available");
    available.setAccessible(true);
    Field holders = DefaultDistributedSemaphoreService.class.getDeclaredField("holders");
    holders.setAccessible(true);
    Field waiterQueue = DefaultDistributedSemaphoreService.class.getDeclaredField("waiterQueue");
    waiterQueue.setAccessible(true);
    Field timers = DefaultDistributedSemaphoreService.class.getDeclaredField("timers");
    timers.setAccessible(true);

    available.set(service, 10);

    Map<Long, Integer> holdersMap = new HashMap<>();
    holdersMap.put((long) 1, 2);
    holdersMap.put((long) 3, 4);
    holdersMap.put((long) 5, 6);
    holders.set(service, holdersMap);

//    Class<?> waiter = Class.forName("io.atomix.core.semaphore.impl.DistributedSemaphoreService$Waiter");
//    LinkedList<Object> waiterLinkedList = new LinkedList<>();
//
//    waiterLinkedList.add(waiter.getConstructors()[0].newInstance(service,10L, 20L, 30L, 40, Long.MAX_VALUE));
//    waiterQueue.set(service, waiterLinkedList);

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    DefaultDistributedSemaphoreService serviceRestore = new DefaultDistributedSemaphoreService(
        new DistributedSemaphoreServiceConfig().setInitialCapacity(10));
    serviceRestore.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    assertEquals(10, available.get(serviceRestore));
    assertEquals(holdersMap, holders.get(serviceRestore));
//    assertEquals(waiterQueue.get(serviceRestore), waiterLinkedList);
//    assertEquals(1, ((Map) (timers.get(serviceRestore))).keySet().size());

  }

}
