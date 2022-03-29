// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue;

import com.google.common.base.MoreObjects;

/**
 * Statistics for a {@link AsyncWorkQueue}.
 */
public final class WorkQueueStats {

  private long totalPending;
  private long totalInProgress;
  private long totalCompleted;

  /**
   * Returns a {@code WorkQueueStats} builder.
   *
   * @return builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private WorkQueueStats() {
  }

  public static class Builder {

    WorkQueueStats workQueueStats = new WorkQueueStats();

    public Builder withTotalPending(long value) {
      workQueueStats.totalPending = value;
      return this;
    }

    public Builder withTotalInProgress(long value) {
      workQueueStats.totalInProgress = value;
      return this;
    }

    public Builder withTotalCompleted(long value) {
      workQueueStats.totalCompleted = value;
      return this;
    }

    public WorkQueueStats build() {
      return workQueueStats;
    }
  }

  /**
   * Returns the total pending tasks. These are the tasks that are added but not yet picked up.
   *
   * @return total pending tasks.
   */
  public long totalPending() {
    return this.totalPending;
  }

  /**
   * Returns the total in progress tasks. These are the tasks that are currently being worked on.
   *
   * @return total in progress tasks.
   */
  public long totalInProgress() {
    return this.totalInProgress;
  }

  /**
   * Returns the total completed tasks.
   *
   * @return total completed tasks.
   */
  public long totalCompleted() {
    return this.totalCompleted;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("totalPending", totalPending)
        .add("totalInProgress", totalInProgress)
        .add("totalCompleted", totalCompleted)
        .toString();
  }
}
