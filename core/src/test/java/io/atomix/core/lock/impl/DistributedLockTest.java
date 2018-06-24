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
package io.atomix.core.lock.impl;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.lock.AsyncDistributedLock;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Version;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft lock test.
 */
public abstract class DistributedLockTest extends AbstractPrimitiveTest {

  /**
   * Tests locking and unlocking a lock.
   */
  @Test
  public void testLockUnlock() throws Throwable {
    AsyncDistributedLock lock = atomix().lockBuilder("test-lock-unlock", protocol()).build().async();
    lock.lock().get(30, TimeUnit.SECONDS);
    lock.unlock().get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests releasing a lock when the client's session is closed.
   */
  @Test
  public void testReleaseOnClose() throws Throwable {
    AsyncDistributedLock lock1 = atomix().lockBuilder("test-lock-on-close", protocol()).build().async();
    AsyncDistributedLock lock2 = atomix().lockBuilder("test-lock-on-close", protocol()).build().async();
    lock1.lock().get(30, TimeUnit.SECONDS);
    CompletableFuture<Version> future = lock2.lock();
    lock1.close();
    future.get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockFail() throws Throwable {
    AsyncDistributedLock lock1 = atomix().lockBuilder("test-try-lock-fail", protocol()).build().async();
    AsyncDistributedLock lock2 = atomix().lockBuilder("test-try-lock-fail", protocol()).build().async();

    lock1.lock().get(30, TimeUnit.SECONDS);

    assertFalse(lock2.tryLock().get(30, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockSucceed() throws Throwable {
    AsyncDistributedLock lock = atomix().lockBuilder("test-try-lock-succeed", protocol()).build().async();
    assertTrue(lock.tryLock().get(30, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockFailWithTimeout() throws Throwable {
    AsyncDistributedLock lock1 = atomix().lockBuilder("test-try-lock-fail-with-timeout", protocol()).build().async();
    AsyncDistributedLock lock2 = atomix().lockBuilder("test-try-lock-fail-with-timeout", protocol()).build().async();

    lock1.lock().get(30, TimeUnit.SECONDS);

    assertFalse(lock2.tryLock(Duration.ofSeconds(1)).get(30, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockSucceedWithTimeout() throws Throwable {
    AsyncDistributedLock lock1 = atomix().lockBuilder("test-try-lock-succeed-with-timeout", protocol()).build().async();
    AsyncDistributedLock lock2 = atomix().lockBuilder("test-try-lock-succeed-with-timeout", protocol()).build().async();

    lock1.lock().get(30, TimeUnit.SECONDS);

    CompletableFuture<Optional<Version>> future = lock2.tryLock(Duration.ofSeconds(1));
    lock1.unlock().get(30, TimeUnit.SECONDS);
    assertTrue(future.get(30, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests unlocking a lock with a blocking call in the event thread.
   */
  @Test
  public void testBlockingUnlock() throws Throwable {
    AsyncDistributedLock lock1 = atomix().lockBuilder("test-blocking-unlock", protocol()).build().async();
    AsyncDistributedLock lock2 = atomix().lockBuilder("test-blocking-unlock", protocol()).build().async();

    lock1.lock().thenRun(() -> {
      Futures.get(lock1.unlock());
    }).get(30, TimeUnit.SECONDS);

    lock2.lock().get(30, TimeUnit.SECONDS);
  }
}
