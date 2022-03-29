// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.config.Config;
import io.atomix.utils.config.ConfigurationException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multicast configuration.
 */
public class MulticastConfig implements Config {
  private static final String DEFAULT_MULTICAST_IP = "230.0.0.1";
  private static final int DEFAULT_MULTICAST_PORT = 54321;

  private boolean enabled = false;
  private InetAddress group;
  private int port = DEFAULT_MULTICAST_PORT;

  public MulticastConfig() {
    try {
      group = InetAddress.getByName(DEFAULT_MULTICAST_IP);
    } catch (UnknownHostException e) {
      group = null;
    }
  }

  /**
   * Returns whether multicast is enabled.
   *
   * @return whether multicast is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Sets whether multicast is enabled.
   *
   * @param enabled whether multicast is enabled
   * @return the multicast configuration
   */
  public MulticastConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Returns the multicast group.
   *
   * @return the multicast group
   */
  public InetAddress getGroup() {
    return group;
  }

  /**
   * Sets the multicast group.
   *
   * @param group the multicast group
   * @return the multicast configuration
   * @throws ConfigurationException if the group is invalid
   */
  public MulticastConfig setGroup(String group) {
    try {
      InetAddress address = InetAddress.getByName(group);
      if (!address.isMulticastAddress()) {
        throw new ConfigurationException("Invalid multicast group " + group);
      }
      return setGroup(address);
    } catch (UnknownHostException e) {
      throw new ConfigurationException("Failed to locate multicast group", e);
    }
  }

  /**
   * Sets the multicast group.
   *
   * @param group the multicast group
   * @return the multicast configuration
   */
  public MulticastConfig setGroup(InetAddress group) {
    this.group = checkNotNull(group);
    return this;
  }

  /**
   * Returns the multicast port.
   *
   * @return the multicast port
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the multicast port.
   *
   * @param port the multicast port
   * @return the multicast configuration
   */
  public MulticastConfig setPort(int port) {
    this.port = port;
    return this;
  }
}
