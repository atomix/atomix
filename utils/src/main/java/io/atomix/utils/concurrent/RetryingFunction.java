/*
 * Copyright 2015-present Open Networking Foundation
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
