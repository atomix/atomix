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
package io.atomix.core.barrier.impl;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cyclic barrier event.
 */
public class CyclicBarrierEvent {
  private final int id;
  private final long version;

  public CyclicBarrierEvent() {
    this(0, 0);
  }

  public CyclicBarrierEvent(int id, long version) {
    this.id = id;
    this.version = version;
  }

  /**
   * Returns the lock ID.
   *
   * @return The lock ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the lock version.
   *
   * @return The lock version.
   */
  public long version() {
    return version;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("version", version)
        .toString();
  }
}
