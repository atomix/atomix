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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * Representation of a network address.
 */
public final class Address {
  private static final int DEFAULT_PORT = 5679;

  /**
   * Address type.
   */
  public enum Type {
    IPV4,
    IPV6,
  }

  /**
   * Returns an address that binds to all interfaces.
   *
   * @return the address
   */
  public static Address local() {
    return from(DEFAULT_PORT);
  }

  /**
   * Returns the address from the given host:port string.
   *
   * @param address the address string
   * @return the address
   */
  public static Address from(String address) {
    int lastColon = address.lastIndexOf(':');
    int openBracket = address.indexOf('[');
    int closeBracket = address.indexOf(']');

    String host;
    if (openBracket != -1 && closeBracket != -1) {
      host = address.substring(openBracket + 1, closeBracket);
    } else if (lastColon != -1) {
      host = address.substring(0, lastColon);
    } else {
      host = address;
    }

    int port;
    if (lastColon != -1) {
      try {
        port = Integer.parseInt(address.substring(lastColon + 1));
      } catch (NumberFormatException e) {
        throw new MalformedAddressException(address, e);
      }
    } else {
      port = DEFAULT_PORT;
    }
    return new Address(host, port);
  }

  /**
   * Returns an address for the given host/port.
   *
   * @param host the host name
   * @param port the port
   * @return a new address
   */
  public static Address from(String host, int port) {
    return new Address(host, port);
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
      return new Address(address.getHostName(), port);
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
  private transient volatile Type type;
  private transient volatile InetAddress address;

  public Address(String host, int port) {
    this(host, port, null);
  }

  public Address(String host, int port, InetAddress address) {
    this.host = host;
    this.port = port;
    this.address = address;
    if (address != null) {
      this.type = address instanceof Inet6Address ? Type.IPV6 : Type.IPV4;
    }
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
    return address(false);
  }

  /**
   * Returns the IP address.
   *
   * @param resolve whether to force resolve the hostname
   * @return the IP address
   */
  public InetAddress address(boolean resolve) {
    if (resolve) {
      address = resolveAddress();
      return address;
    }

    if (address == null) {
      synchronized (this) {
        if (address == null) {
          address = resolveAddress();
        }
      }
    }
    return address;
  }

  /**
   * Resolves the IP address from the hostname.
   *
   * @return the resolved IP address or {@code null} if the IP could not be resolved
   */
  private InetAddress resolveAddress() {
    try {
      return InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      return null;
    }
  }

  /**
   * Returns the address type.
   *
   * @return the address type
   */
  public Type type() {
    if (type == null) {
      synchronized (this) {
        if (type == null) {
          type = address() instanceof Inet6Address ? Type.IPV6 : Type.IPV4;
        }
      }
    }
    return type;
  }

  @Override
  public String toString() {
    String host = host();
    int port = port();
    if (host.matches("([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}")) {
      return String.format("[%s]:%d", host, port);
    } else {
      return String.format("%s:%d", host, port);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Address)) {
      return false;
    }
    Address that = (Address) obj;
    return this.host.equals(that.host)
        && this.port == that.port;
  }
}
