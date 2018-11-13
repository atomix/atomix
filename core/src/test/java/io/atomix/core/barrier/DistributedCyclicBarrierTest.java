/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.barrier;

import io.atomix.core.AbstractPrimitiveTest;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distributed cyclic barrier test.
 */
public class DistributedCyclicBarrierTest extends AbstractPrimitiveTest {
  @Test
  @Ignore
  public void testBarrier() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    AsyncDistributedCyclicBarrier barrier1 = atomix().cyclicBarrierBuilder("test-barrier")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncDistributedCyclicBarrier barrier2 = atomix().cyclicBarrierBuilder("test-barrier")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncDistributedCyclicBarrier barrier3 = atomix().cyclicBarrierBuilder("test-barrier")
        .withProtocol(protocol())
        .withBarrierAction(() -> latch.countDown())
        .build()
        .async();

    assertTrue(barrier1.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier2.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier3.getParties().get(10, TimeUnit.SECONDS) == 3);

    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);

    CompletableFuture<Integer> future1 = barrier1.await();
    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);

    CompletableFuture<Integer> future2 = barrier2.await();
    CompletableFuture<Integer> future3 = barrier3.await();

    future1.get(10, TimeUnit.SECONDS);
    future2.get(10, TimeUnit.SECONDS);
    future3.get(10, TimeUnit.SECONDS);

    latch.await(10, TimeUnit.SECONDS);

    assertTrue(barrier1.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier2.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier3.getParties().get(10, TimeUnit.SECONDS) == 3);

    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);

    future1 = barrier1.await();
    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);

    future2 = barrier2.await();
    future3 = barrier3.await();

    future1.get(10, TimeUnit.SECONDS);
    future2.get(10, TimeUnit.SECONDS);
    future3.get(10, TimeUnit.SECONDS);
  }

  @Test
  public void testBrokenBarrierReset() throws Exception {
    AsyncDistributedCyclicBarrier barrier1 = atomix().cyclicBarrierBuilder("test-barrier-reset")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncDistributedCyclicBarrier barrier2 = atomix().cyclicBarrierBuilder("test-barrier-reset")
        .withProtocol(protocol())
        .build()
        .async();
    AsyncDistributedCyclicBarrier barrier3 = atomix().cyclicBarrierBuilder("test-barrier-reset")
        .withProtocol(protocol())
        .build()
        .async();

    CompletableFuture<Integer> future1 = barrier1.await();
    CompletableFuture<Integer> future2 = barrier2.await(Duration.ofMillis(500));

    try {
      future1.get(10, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof BrokenBarrierException);
    }

    try {
      future2.get(10, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof BrokenBarrierException);
    }

    CompletableFuture<Integer> future3 = barrier1.await();

    try {
      future3.get(10, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof BrokenBarrierException);
    }

    assertTrue(barrier1.isBroken().get(10, TimeUnit.SECONDS));

    barrier1.reset().get(10, TimeUnit.SECONDS);

    assertFalse(barrier1.isBroken().get(10, TimeUnit.SECONDS));

    assertTrue(barrier1.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier2.getParties().get(10, TimeUnit.SECONDS) == 3);
    assertTrue(barrier3.getParties().get(10, TimeUnit.SECONDS) == 3);

    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 0);

    future1 = barrier1.await();
    int waiting = barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS);
    assertTrue(barrier1.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier2.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);
    assertTrue(barrier3.getNumberWaiting().get(10, TimeUnit.SECONDS) == 1);

    future2 = barrier2.await();
    future3 = barrier3.await();

    future1.get(10, TimeUnit.SECONDS);
    future2.get(10, TimeUnit.SECONDS);
    future3.get(10, TimeUnit.SECONDS);
  }
}
