/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.vertx.protocol.impl;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriPath;
import net.kuujo.copycat.uri.UriPort;
import net.kuujo.copycat.uri.UriSchemeSpecificPart;

import org.vertx.java.core.Vertx;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocol implements Protocol {
  private String host;
  private int port;
  private Vertx vertx;
  private String address;

  public EventBusProtocol() {
  }

  public EventBusProtocol(String host, int port, String address) {
    this.host = host;
    this.port = port;
    this.address = address;
  }

  public EventBusProtocol(Vertx vertx, String address) {
    this.vertx = vertx;
    this.address = address;
  }

  /**
   * Sets the event bus address.
   *
   * @param address The event bus address.
   */
  @UriPath
  @UriSchemeSpecificPart
  public void setAddress(String address) {
    this.address = address;
  }

  /**
   * Returns the event bus address.
   *
   * @return The event bus address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the event bus address, returning the protocol for method chaining.
   *
   * @param address The event bus address.
   * @return The event bus protocol.
   */
  public EventBusProtocol withAddress(String address) {
    this.address = address;
    return this;
  }

  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public EventBusProtocol withHost(String host) {
    this.host = host;
    return this;
  }

  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  public EventBusProtocol withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public ProtocolServer createServer() {
    if (vertx != null) {
      return new EventBusProtocolServer(address, vertx);
    } else {
      return new EventBusProtocolServer(address, host, port);
    }
  }

  @Override
  public ProtocolClient createClient() {
    if (vertx != null) {
      return new EventBusProtocolClient(address, vertx);
    } else {
      return new EventBusProtocolClient(address, host, port);
    }
  }

}
