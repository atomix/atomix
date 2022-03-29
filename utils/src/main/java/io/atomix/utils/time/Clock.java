// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

/**
 * Clock.
 */
public interface Clock<T extends Timestamp> {

  /**
   * Returns the current time of the clock.
   *
   * @return the current time
   */
  T getTime();

}
