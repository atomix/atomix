// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock;

import io.atomix.core.AbstractPrimitiveTest;
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
public class AtomicLockTest extends AbstractPrimitiveTest {

  /**
   * Tests locking and unlocking a lock.
   */
  @Test
  public void testLockUnlock() throws Throwable {
    AtomicLock lock = atomix().atomicLockBuilder("test-lock-unlock")
        .withProtocol(protocol())
        .build();
    Version version = lock.lock();
    assertTrue(lock.isLocked());
    assertTrue(lock.isLocked(version));
    assertFalse(lock.isLocked(new Version(version.value() + 1)));
    lock.unlock();
    assertFalse(lock.isLocked());
    assertFalse(lock.isLocked(version));
  }

  /**
   * Tests releasing a lock when the client's session is closed.
   */
  @Test
  public void testReleaseOnClose() throws Throwable {
    AtomicLock lock1 = atomix().atomicLockBuilder("test-lock-on-close")
        .withProtocol(protocol())
        .build();
    AtomicLock lock2 = atomix().atomicLockBuilder("test-lock-on-close")
        .withProtocol(protocol())
        .build();
    lock1.lock();
    CompletableFuture<Version> future = lock2.async().lock();
    lock1.close();
    future.get(10, TimeUnit.SECONDS);
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockFail() throws Throwable {
    AtomicLock lock1 = atomix().atomicLockBuilder("test-try-lock-fail")
        .withProtocol(protocol())
        .build();
    AtomicLock lock2 = atomix().atomicLockBuilder("test-try-lock-fail")
        .withProtocol(protocol())
        .build();

    lock1.lock();

    assertFalse(lock2.tryLock().isPresent());
  }

  /**
   * Tests attempting to acquire a lock.
   */
  @Test
  public void testTryLockSucceed() throws Throwable {
    AtomicLock lock = atomix().atomicLockBuilder("test-try-lock-succeed")
        .withProtocol(protocol())
        .build();
    assertTrue(lock.tryLock().isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockFailWithTimeout() throws Throwable {
    AtomicLock lock1 = atomix().atomicLockBuilder("test-try-lock-fail-with-timeout")
        .withProtocol(protocol())
        .build();
    AtomicLock lock2 = atomix().atomicLockBuilder("test-try-lock-fail-with-timeout")
        .withProtocol(protocol())
        .build();

    lock1.lock();

    assertFalse(lock2.tryLock(Duration.ofSeconds(1)).isPresent());
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  @Test
  public void testTryLockSucceedWithTimeout() throws Throwable {
    AtomicLock lock1 = atomix().atomicLockBuilder("test-try-lock-succeed-with-timeout")
        .withProtocol(protocol())
        .build();
    AtomicLock lock2 = atomix().atomicLockBuilder("test-try-lock-succeed-with-timeout")
        .withProtocol(protocol())
        .build();

    lock1.lock();

    CompletableFuture<Optional<Version>> future = lock2.async().tryLock(Duration.ofSeconds(1));
    lock1.unlock();
    assertTrue(future.get(10, TimeUnit.SECONDS).isPresent());
  }

  /**
   * Tests unlocking a lock with a blocking call in the event thread.
   */
  @Test
  public void testBlockingUnlock() throws Throwable {
    AtomicLock lock1 = atomix().atomicLockBuilder("test-blocking-unlock")
        .withProtocol(protocol())
        .build();
    AtomicLock lock2 = atomix().atomicLockBuilder("test-blocking-unlock")
        .withProtocol(protocol())
        .build();

    lock1.async().lock().thenRun(() -> lock1.unlock());

    lock2.lock();
  }
}
