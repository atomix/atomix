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
package io.atomix.primitives.counter;

import io.atomix.primitives.DistributedPrimitive;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicLong.
 */
public interface AtomicCounter extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.COUNTER;
  }

  /**
   * Atomically increment by one the current value.
   *
   * @return updated value
   */
  long incrementAndGet();

  /**
   * Atomically increment by one the current value.
   *
   * @return previous value
   */
  long getAndIncrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return previous value
   */
  long getAndAdd(long delta);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return updated value
   */
  long addAndGet(long delta);

  /**
   * Atomically sets the given value to the current value.
   *
   * @param value the value to set
   */
  void set(long value);

  /**
   * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
   * no change occurs.
   *
   * @param expectedValue the expected current value of the counter
   * @param updateValue   the new value to be set
   * @return true if the update occurred and the expected value was equal to the current value, false otherwise
   */
  boolean compareAndSet(long expectedValue, long updateValue);

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  long get();
}
