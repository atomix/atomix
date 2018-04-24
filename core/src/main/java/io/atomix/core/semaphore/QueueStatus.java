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
package io.atomix.core.semaphore;

import com.google.common.base.MoreObjects;

public class QueueStatus {
  private int queueLength;
  private int totalPermits;

  public QueueStatus(int queueLength, int totalPermits) {
    this.queueLength = queueLength;
    this.totalPermits = totalPermits;
  }

  /**
   * Get the count of requests waiting for permits.
   *
   * @return count of requests waiting for permits
   */
  public int queueLength() {
    return queueLength;
  }

  /**
   * Get the sum of permits waiting for.
   *
   * @return sum of permits
   */
  public int totalPermits() {
    return totalPermits;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("queueLength", queueLength)
            .add("totalPermits", totalPermits)
            .toString();
  }
}
