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
package io.atomix.core.counter.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Atomic counter service.
 */
public interface AtomicCounterService {

  /**
   * Atomically increment by one and return the updated value.
   *
   * @return updated value
   */
  @Command
  long incrementAndGet();

  /**
   * Atomically decrement by one and return the updated value.
   *
   * @return updated value
   */
  @Command
  long decrementAndGet();

  /**
   * Atomically increment by one and return the previous value.
   *
   * @return previous value
   */
  @Command
  long getAndIncrement();

  /**
   * Atomically decrement by one and return the previous value.
   *
   * @return previous value
   */
  @Command
  long getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return previous value
   */
  @Command
  long getAndAdd(long delta);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return updated value
   */
  @Command
  long addAndGet(long delta);

  /**
   * Atomically sets the given value to the current value.
   *
   * @param value the value to set
   */
  @Command
  void set(long value);

  /**
   * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
   * no change occurs.
   *
   * @param expectedValue the expected current value of the counter
   * @param updateValue   the new value to be set
   * @return true if the update occurred and the expected value was equal to the current value, false otherwise
   */
  @Command
  boolean compareAndSet(long expectedValue, long updateValue);

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  @Query
  long get();

}
