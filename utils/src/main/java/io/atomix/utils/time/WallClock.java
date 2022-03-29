// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Wall clock.
 */
public class WallClock implements Clock<WallClockTimestamp> {
  @Override
  public WallClockTimestamp getTime() {
    return new WallClockTimestamp();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("time", getTime())
        .toString();
  }
}
