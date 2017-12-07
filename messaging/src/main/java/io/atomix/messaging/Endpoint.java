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
package io.atomix.messaging;

import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * Representation of a TCP/UDP communication end point.
 */
public final class Endpoint {

  /**
   * Returns an endpoint for the given host/port.
   *
   * @param host the host
   * @param port the port
   * @return a new endpoint
   */
  public static Endpoint from(String host, int port) {
    try {
      return new Endpoint(InetAddress.getByName(host), port);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Failed to locate host", e);
    }
  }

  private final int port;
  private final InetAddress ip;

  public Endpoint(InetAddress host, int port) {
    this.ip = Preconditions.checkNotNull(host);
    this.port = port;
  }

  public InetAddress host() {
    return ip;
  }

  public int port() {
    return port;
  }

  @Override
  public String toString() {
    return String.format("%s:%d", host().getHostAddress(), port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, port);
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
    Endpoint that = (Endpoint) obj;
    return this.port == that.port &&
        Objects.equals(this.ip, that.ip);
  }
}
