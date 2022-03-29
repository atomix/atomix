// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import java.time.Duration;

/**
 * SWIM membership protocol builder.
 */
public class SwimMembershipProtocolBuilder extends GroupMembershipProtocolBuilder {
  private final SwimMembershipProtocolConfig config = new SwimMembershipProtocolConfig();

  /**
   * Sets whether to broadcast member updates to all peers.
   *
   * @param broadcastUpdates whether to broadcast member updates to all peers
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withBroadcastUpdates(boolean broadcastUpdates) {
    config.setBroadcastUpdates(broadcastUpdates);
    return this;
  }

  /**
   * Sets whether to broadcast disputes to all peers.
   *
   * @param broadcastDisputes whether to broadcast disputes to all peers
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withBroadcastDisputes(boolean broadcastDisputes) {
    config.setBroadcastDisputes(broadcastDisputes);
    return this;
  }

  /**
   * Sets whether to notify a suspect node on state changes.
   *
   * @param notifySuspect whether to notify a suspect node on state changes
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withNotifySuspect(boolean notifySuspect) {
    config.setNotifySuspect(notifySuspect);
    return this;
  }

  /**
   * Sets the gossip interval.
   *
   * @param gossipInterval the gossip interval
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withGossipInterval(Duration gossipInterval) {
    config.setGossipInterval(gossipInterval);
    return this;
  }

  /**
   * Sets the gossip fanout.
   *
   * @param gossipFanout the gossip fanout
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withGossipFanout(int gossipFanout) {
    config.setGossipFanout(gossipFanout);
    return this;
  }

  /**
   * Sets the probe interval.
   *
   * @param probeInterval the probe interval
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withProbeInterval(Duration probeInterval) {
    config.setProbeInterval(probeInterval);
    return this;
  }

  /**
   * Sets the number of probes to perform on suspect members.
   *
   * @param suspectProbes the number of probes to perform on suspect members
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withSuspectProbes(int suspectProbes) {
    config.setSuspectProbes(suspectProbes);
    return this;
  }

  /**
   * Sets the failure timeout to use prior to phi failure detectors being populated.
   *
   * @param failureTimeout the failure timeout
   * @return the protocol builder
   */
  public SwimMembershipProtocolBuilder withFailureTimeout(Duration failureTimeout) {
    config.setFailureTimeout(failureTimeout);
    return this;
  }

  @Override
  public GroupMembershipProtocol build() {
    return new SwimMembershipProtocol(config);
  }
}
