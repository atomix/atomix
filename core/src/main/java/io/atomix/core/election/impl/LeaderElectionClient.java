// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election.impl;

import io.atomix.core.election.Leadership;
import io.atomix.primitive.event.Event;

/**
 * Leader election client.
 */
public interface LeaderElectionClient {

  /**
   * Called when a leadership change event occurs.
   *
   * @param oldLeadership the old leadership
   * @param newLeadership the new leadership
   */
  @Event
  void onLeadershipChange(Leadership<byte[]> oldLeadership, Leadership<byte[]> newLeadership);

}
