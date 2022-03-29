// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Ordered completable future test.
 */
public class OrderedFutureTest {

  /**
   * Tests ordered completion of future callbacks.
   */
  @Test
  public void testOrderedCompletion() throws Throwable {
    CompletableFuture<String> future = new OrderedFuture<>();
    AtomicInteger order = new AtomicInteger();
    future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
    future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
    future.handle((r, e) -> {
      assertEquals(3, order.incrementAndGet());
      assertEquals("foo", r);
      return "bar";
    });
    future.thenRun(() -> assertEquals(3, order.incrementAndGet()));
    future.thenAccept(r -> {
      assertEquals(5, order.incrementAndGet());
      assertEquals("foo", r);
    });
    future.thenApply(r -> {
      assertEquals(6, order.incrementAndGet());
      assertEquals("foo", r);
      return "bar";
    });
    future.whenComplete((r, e) -> {
      assertEquals(7, order.incrementAndGet());
      assertEquals("foo", r);
    });
    future.complete("foo");
  }

  /**
   * Tests ordered failure of future callbacks.
   */
  public void testOrderedFailure() throws Throwable {
    CompletableFuture<String> future = new OrderedFuture<>();
    AtomicInteger order = new AtomicInteger();
    future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
    future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
    future.handle((r, e) -> {
      assertEquals(3, order.incrementAndGet());
      return "bar";
    });
    future.thenRun(() -> fail());
    future.thenAccept(r -> fail());
    future.exceptionally(e -> {
      assertEquals(3, order.incrementAndGet());
      return "bar";
    });
    future.completeExceptionally(new RuntimeException("foo"));
  }

  /**
   * Tests calling callbacks that are added after completion.
   */
  public void testAfterComplete() throws Throwable {
    CompletableFuture<String> future = new OrderedFuture<>();
    future.whenComplete((result, error) -> assertEquals("foo", result));
    future.complete("foo");
    AtomicInteger count = new AtomicInteger();
    future.whenComplete((result, error) -> {
      assertEquals("foo", result);
      assertEquals(1, count.incrementAndGet());
    });
    future.thenAccept(result -> {
      assertEquals("foo", result);
      assertEquals(2, count.incrementAndGet());
    });
    assertEquals(2, count.get());
  }
}
