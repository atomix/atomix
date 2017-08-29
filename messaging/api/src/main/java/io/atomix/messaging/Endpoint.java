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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of a TCP/UDP communication end point.
 */
public final class Endpoint {
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
    return MoreObjects.toStringHelper(getClass())
        .add("ip", ip)
        .add("port", port)
        .toString();
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
