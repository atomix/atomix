// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election.impl;

import io.atomix.core.election.Leadership;
import io.atomix.primitive.event.Event;

/**
 * Leader elector client.
 */
public interface LeaderElectorClient {

  /**
   * Called when a leadership change occurs.
   *
   * @param topic the topic for which the leadership change occurred
   * @param oldLeadership the old leadership
   * @param newLeadership the new leadership
   */
  @Event
  void onLeadershipChange(String topic, Leadership<byte[]> oldLeadership, Leadership<byte[]> newLeadership);

}
