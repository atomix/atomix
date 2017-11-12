/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitives.lock;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder for AtomicIdGenerator.
 */
public abstract class DistributedLockBuilder
    extends DistributedPrimitiveBuilder<DistributedLockBuilder, DistributedLock, AsyncDistributedLock> {

  private Duration lockTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);

  public DistributedLockBuilder() {
    super(DistributedPrimitive.Type.LOCK);
  }

  /**
   * Sets the lock timeout in milliseconds.
   *
   * @param lockTimeoutMillis the lock timeout in milliseconds
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(long lockTimeoutMillis) {
    return withLockTimeout(Duration.ofMillis(lockTimeoutMillis));
  }

  /**
   * Sets the lock timeout.
   *
   * @param lockTimeout the lock timeout
   * @param timeUnit        the timeout time unit
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(long lockTimeout, TimeUnit timeUnit) {
    return withLockTimeout(Duration.ofMillis(timeUnit.toMillis(lockTimeout)));
  }

  /**
   * Sets the lock timeout.
   *
   * @param lockTimeout the lock timeout
   * @return leader elector builder
   */
  public DistributedLockBuilder withLockTimeout(Duration lockTimeout) {
    this.lockTimeout = checkNotNull(lockTimeout);
    return this;
  }

  /**
   * Returns the lock timeout.
   *
   * @return the lock timeout
   */
  public Duration lockTimeout() {
    return lockTimeout;
  }

  @Override
  public DistributedLock build() {
    return buildAsync().asDistributedLock();
  }
}