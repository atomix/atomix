// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Function that retries execution on failure.
 *
 * @param <U> input type
 * @param <V> output type
 */
public class RetryingFunction<U, V> implements Function<U, V> {
  private final Function<U, V> baseFunction;
  private final Class<? extends Throwable> exceptionClass;
  private final int maxRetries;
  private final int maxDelayBetweenRetries;

  public RetryingFunction(Function<U, V> baseFunction,
      Class<? extends Throwable> exceptionClass,
      int maxRetries,
      int maxDelayBetweenRetries) {
    this.baseFunction = baseFunction;
    this.exceptionClass = exceptionClass;
    this.maxRetries = maxRetries;
    this.maxDelayBetweenRetries = maxDelayBetweenRetries;
  }

  @SuppressWarnings("squid:S1181")
  // Yes we really do want to catch Throwable
  @Override
  public V apply(U input) {
    int retryAttempts = 0;
    while (true) {
      try {
        return baseFunction.apply(input);
      } catch (Throwable t) {
        if (!exceptionClass.isAssignableFrom(t.getClass()) || retryAttempts == maxRetries) {
          throwIfUnchecked(t);
          throw new RuntimeException(t);
        }
        Retries.randomDelay(maxDelayBetweenRetries);
        retryAttempts++;
      }
    }
  }
}
