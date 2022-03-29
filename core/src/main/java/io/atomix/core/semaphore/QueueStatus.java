// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
