// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.utils.time.Timestamp;
import io.atomix.utils.time.WallClockTimestamp;

/**
 * Configurable timestamp provider implementations.
 */
@Beta
public enum TimestampProviders implements TimestampProvider {
  WALL_CLOCK {
    @Override
    public Timestamp get(Object entry) {
      return new WallClockTimestamp();
    }
  }
}
