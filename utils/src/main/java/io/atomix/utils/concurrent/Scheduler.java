// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler.
 */
public interface Scheduler {

  /**
   * Schedules a runnable after a delay.
   *
   * @param delay the delay after which to run the callback
   * @param timeUnit the time unit
   * @param callback the callback to run
   * @return the scheduled callback
   */
  default Scheduled schedule(long delay, TimeUnit timeUnit, Runnable callback) {
    return schedule(Duration.ofMillis(timeUnit.toMillis(delay)), callback);
  }

  /**
   * Schedules a runnable after a delay.
   *
   * @param delay the delay after which to run the callback
   * @param callback the callback to run
   * @return the scheduled callback
   */
  Scheduled schedule(Duration delay, Runnable callback);

  /**
   * Schedules a runnable at a fixed rate.
   *
   * @param initialDelay the initial delay
   * @param interval the interval at which to run the callback
   * @param timeUnit the time unit
   * @param callback the callback to run
   * @return the scheduled callback
   */
  default Scheduled schedule(long initialDelay, long interval, TimeUnit timeUnit, Runnable callback) {
    return schedule(Duration.ofMillis(timeUnit.toMillis(initialDelay)), Duration.ofMillis(timeUnit.toMillis(interval)), callback);
  }

  /**
   * Schedules a runnable at a fixed rate.
   *
   * @param initialDelay the initial delay
   * @param interval the interval at which to run the callback
   * @param callback the callback to run
   * @return the scheduled callback
   */
  Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback);

}
