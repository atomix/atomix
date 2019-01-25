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
package io.atomix.core.semaphore;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Semaphore test.
 */
public class DistributedSemaphoreTest extends AbstractPrimitiveTest {

  @Test(timeout = 30000)
  public void testInit() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore100 = atomix.semaphoreBuilder("test-semaphore-init-100")
        .withProtocol(protocol())
        .withInitialCapacity(100)
        .build();
    DistributedSemaphore semaphore0 = atomix.semaphoreBuilder("test-semaphore-init-0")
        .withProtocol(protocol())
        .build();
    assertEquals(100, semaphore100.availablePermits());
    assertEquals(0, semaphore0.availablePermits());

    DistributedSemaphore semaphoreNoInit = atomix.semaphoreBuilder("test-semaphore-init-100")
        .withProtocol(protocol())
        .build();
    assertEquals(100, semaphoreNoInit.availablePermits());
  }

  @Test(timeout = 30000)
  public void testAcquireRelease() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-base")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    assertEquals(10, semaphore.availablePermits());
    semaphore.acquire();
    assertEquals(9, semaphore.availablePermits());
    semaphore.acquire(9);
    assertEquals(0, semaphore.availablePermits());

    semaphore.release();
    assertEquals(1, semaphore.availablePermits());
    semaphore.release(100);
    assertEquals(101, semaphore.availablePermits());
  }

  @Test(timeout = 30000)
  public void testIncreaseReduceDrain() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-ird")
        .withProtocol(protocol())
        .withInitialCapacity(-10)
        .build();

    assertEquals(-10, semaphore.availablePermits());
    semaphore.increasePermits(20);
    assertEquals(10, semaphore.availablePermits());
    semaphore.reducePermits(20);
    assertEquals(-10, semaphore.availablePermits());
    assertEquals(-10, semaphore.drainPermits());
    assertEquals(0, semaphore.availablePermits());
  }

  @Test(timeout = 30000)
  public void testOverflow() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-overflow")
        .withProtocol(protocol())
        .withInitialCapacity(Integer.MAX_VALUE)
        .build();

    semaphore.increasePermits(10);
    semaphore.release(10);
    assertEquals(Integer.MAX_VALUE, semaphore.availablePermits());
    assertEquals(Integer.MAX_VALUE, semaphore.drainPermits());

    semaphore.reducePermits(Integer.MAX_VALUE);
    semaphore.reducePermits(Integer.MAX_VALUE);
    assertEquals(Integer.MIN_VALUE, semaphore.availablePermits());
  }

  @Test(timeout = 10000)
  public void testTimeout() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-timeout")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    assertFalse(semaphore.tryAcquire(11));
    assertTrue(semaphore.tryAcquire(10));

    long start = System.currentTimeMillis();
    assertFalse(semaphore.tryAcquire(Duration.ofSeconds(1)));
    long end = System.currentTimeMillis();
    assertTrue(end - start >= 1000);
    assertTrue(end - start < 1100);

    semaphore.release();
    start = System.currentTimeMillis();
    assertTrue(semaphore.tryAcquire(Duration.ofMillis(100)));
    end = System.currentTimeMillis();
    assertTrue(end - start < 100);

    CompletableFuture<Boolean> future = semaphore.async().tryAcquire(Duration.ofSeconds(1));
    semaphore.release();
    assertTrue(future.get(10, TimeUnit.SECONDS));
  }

  @Test(timeout = 30000)
  public void testBlocking() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-blocking")
            .withProtocol(protocol())
            .withInitialCapacity(10)
            .build();

    semaphore.acquire();
    assertEquals(9, semaphore.availablePermits());

    semaphore.release();
    assertEquals(10, semaphore.availablePermits());

    semaphore.increasePermits(10);
    assertEquals(20, semaphore.availablePermits());

    semaphore.reducePermits(10);
    assertEquals(10, semaphore.availablePermits());

    assertFalse(semaphore.tryAcquire(11));
    assertFalse(semaphore.tryAcquire(11, Duration.ofMillis(1)));
    assertTrue(semaphore.tryAcquire(5));

    assertEquals(5, semaphore.drainPermits());

    DistributedSemaphore semaphore2 =
        atomix.semaphoreBuilder("test-semaphore-blocking")
            .withProtocol(protocol())
            .withInitialCapacity(10)
            .build();

    assertEquals(0, semaphore2.availablePermits());
    semaphore.close();
    assertEquals(10, semaphore2.availablePermits());
  }

  @Test(timeout = 10000)
  public void testQueue() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-queue")
            .withProtocol(protocol())
            .withInitialCapacity(10)
            .build();

    CompletableFuture<Void> future20 = semaphore.async().acquire(20);
    CompletableFuture<Void> future11 = semaphore.async().acquire(11);

    semaphore.increasePermits(1);
    assertNotNull(future11);
    assertFalse(future20.isDone());
    assertEquals(0, semaphore.availablePermits());

    CompletableFuture<Void> future21 = semaphore.async().acquire(21);
    CompletableFuture<Void> future5 = semaphore.async().acquire(5);

    // wakeup 5 and 20
    semaphore.release(25);
    future5.get(10, TimeUnit.SECONDS);
    future20.get(10, TimeUnit.SECONDS);
    assertFalse(future21.isDone());
  }

  @Test(timeout = 30000)
  public void testExpire() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore =
        atomix.semaphoreBuilder("test-semaphore-expire")
            .withProtocol(protocol())
            .withInitialCapacity(10)
            .build();

    assertEquals(10, semaphore.availablePermits());

    CompletableFuture<Boolean> future = semaphore.async().tryAcquire(11, Duration.ofMillis(500));
    Thread.sleep(500);
    assertFalse(future.get(10, TimeUnit.SECONDS));
    assertEquals(10, semaphore.availablePermits());
  }

  @Test(timeout = 10000)
  public void testInterrupt() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-interrupt")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    DistributedSemaphore semaphore2 = atomix.semaphoreBuilder("test-semaphore-interrupt")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    AtomicBoolean interrupted = new AtomicBoolean();

    Thread t = new Thread(() -> {
      try {
        semaphore.acquire(11);
      } catch (InterruptedException e) {
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
    semaphore2.increasePermits(1);

    // wait asynchronous release.
    Thread.sleep(1000);
    assertEquals(11, semaphore.availablePermits());
  }

  @Test(timeout = 30000)
  public void testBlockTimeout() throws Exception {
    Atomix atomix = atomix();
    DistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-block-timeout")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    DistributedSemaphore semaphore2 = atomix.semaphoreBuilder("test-semaphore-block-timeout")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build();

    Object timedout = new Object();

    Thread t = new Thread(() -> {
      try {
        semaphore.acquire(11);
      } catch (PrimitiveException.Timeout | InterruptedException e) {
        synchronized (timedout) {
          timedout.notifyAll();
        }
      }
    });
    t.start();

    synchronized (timedout) {
      timedout.wait();
    }

    semaphore2.increasePermits(1);

    // wait asynchronous release.
    Thread.sleep(1000);
    assertEquals(11, semaphore.availablePermits());
  }

  @Test(timeout = 60000)
  public void testExpireRace() throws Exception {
    int testCount = 10000;
    int threads = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    Atomix atomix = atomix();

    List<Future<?>> taskFuture = new ArrayList<>(threads);
    AtomicInteger acquired = new AtomicInteger();

    for (int i = 0; i < threads; i++) {
      taskFuture.add(executorService.submit(() -> {
        DistributedSemaphore semaphore =
            atomix.semaphoreBuilder("test-semaphore-race")
                .withProtocol(protocol())
                .withInitialCapacity(testCount)
                .build();
        while (acquired.get() < testCount) {
          try {
            if (semaphore.tryAcquire(Duration.ofMillis(1))) {
              acquired.incrementAndGet();
            }
          } catch (InterruptedException e) {
            fail();
          }
        }
      }));
    }

    for (Future<?> future : taskFuture) {
      future.get();
    }

    executorService.shutdown();
  }

}
