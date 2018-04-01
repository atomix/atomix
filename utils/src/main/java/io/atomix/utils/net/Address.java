/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.utils.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * Representation of a network address.
 */
public final class Address {

  /**
   * Returns the address from the given host:port string.
   *
   * @param address the address string
   * @return the address
   */
  public static Address from(String address) {
    String[] hostAndPort = address.split(":");

    if (hostAndPort.length != 2) {
      throw new MalformedAddressException(address);
    }

    String host = hostAndPort[0];
    int port;
    try {
      port = Integer.parseInt(hostAndPort[1]);
    } catch (NumberFormatException e) {
      throw new MalformedAddressException(address, e);
    }

    try {
      return new Address(host, port, InetAddress.getByName(host));
    } catch (UnknownHostException e) {
      throw new MalformedAddressException(address, e);
    }
  }

  /**
   * Returns an address for the given host/port.
   *
   * @param host the host name
   * @param port the port
   * @return a new address
   */
  public static Address from(String host, int port) {
    try {
      return new Address(host, port, InetAddress.getByName(host));
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Failed to locate host", e);
    }
  }

  /**
   * Returns an address for the local host and the given port.
   *
   * @param port the port
   * @return a new address
   */
  public static Address from(int port) {
    try {
      InetAddress address = getLocalAddress();
      return new Address(address.getHostName(), port, address);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Failed to locate host", e);
    }
  }

  /**
   * Returns the local host.
   */
  private static InetAddress getLocalAddress() throws UnknownHostException {
    try {
      return InetAddress.getLocalHost();  // first NIC
    } catch (Exception ignore) {
      return InetAddress.getByName(null);
    }
  }

  private final String host;
  private final int port;
  private final InetAddress address;

  public Address(String host, int port, InetAddress address) {
    this.host = host;
    this.port = port;
    this.address = address;
  }

  /**
   * Returns the host name.
   *
   * @return the host name
   */
  public String host() {
    return host;
  }

  /**
   * Returns the port.
   *
   * @return the port
   */
  public int port() {
    return port;
  }

  /**
   * Returns the IP address.
   *
   * @return the IP address
   */
  public InetAddress address() {
    return address;
  }

  @Override
  public String toString() {
    return String.format("%s:%d", host(), port());
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, port);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Address that = (Address) obj;
    return this.port == that.port &&
        Objects.equals(this.address, that.address);
  }
}
