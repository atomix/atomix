// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Retry utilities.
 */
public final class Retries {

  /**
   * Returns a function that retries execution on failure.
   * @param base base function
   * @param exceptionClass type of exception for which to retry
   * @param maxRetries max number of retries before giving up
   * @param maxDelayBetweenRetries max delay between successive retries. The actual delay is randomly picked from
   * the interval (0, maxDelayBetweenRetries]
   * @return function
   * @param <U> type of function input
   * @param <V> type of function output
   */
  public static <U, V> Function<U, V> retryable(Function<U, V> base,
                                                Class<? extends Throwable> exceptionClass,
                                                int maxRetries,
                                                int maxDelayBetweenRetries) {
    return new RetryingFunction<>(base, exceptionClass, maxRetries, maxDelayBetweenRetries);
  }

  /**
   * Returns a Supplier that retries execution on failure.
   * @param base base supplier
   * @param exceptionClass type of exception for which to retry
   * @param maxRetries max number of retries before giving up
   * @param maxDelayBetweenRetries max delay between successive retries. The actual delay is randomly picked from
   * the interval (0, maxDelayBetweenRetries]
   * @return supplier
   * @param <V> type of supplied result
   */
  public static <V> Supplier<V> retryable(Supplier<V> base,
                                          Class<? extends Throwable> exceptionClass,
                                          int maxRetries,
                                          int maxDelayBetweenRetries) {
    return () -> new RetryingFunction<>(v -> base.get(),
        exceptionClass,
        maxRetries,
        maxDelayBetweenRetries).apply(null);
  }

  /**
   * Suspends the current thread for a random number of millis between 0 and
   * the indicated limit.
   *
   * @param ms max number of millis
   */
  public static void randomDelay(int ms) {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(ms));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
  }

  /**
   * Suspends the current thread for a specified number of millis and nanos.
   *
   * @param ms    number of millis
   * @param nanos number of nanos
   */
  public static void delay(int ms, int nanos) {
    try {
      Thread.sleep(ms, nanos);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
  }

  private Retries() {
  }

}
