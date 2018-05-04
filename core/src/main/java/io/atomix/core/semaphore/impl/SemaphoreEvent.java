/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import com.google.common.base.MoreObjects;

public class SemaphoreEvent {
  private final long id;
  private final long version;
  private int permits;

  public SemaphoreEvent(long id, long version, int permits) {
    this.id = id;
    this.version = version;
    this.permits = permits;
  }

  /**
   * Returns the semaphore operation ID.
   *
   * @return The semaphore operation ID.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the semaphore version.
   *
   * @return The semaphore version.
   */
  public long version() {
    return version;
  }


  /**
   * Return the permits acquired or released.
   *
   * @return The permits acquired or released
   */
  public int permits() {
    return permits;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("version", version)
            .add("permits", permits)
            .toString();
  }
}
