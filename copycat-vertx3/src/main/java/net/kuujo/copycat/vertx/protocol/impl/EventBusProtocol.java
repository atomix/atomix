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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.util.concurrent.CountDownLatch;

import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.spi.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.spi.protocol.ProtocolServer;
import net.kuujo.copycat.uri.UriAuthority;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriPort;
import net.kuujo.copycat.uri.UriQueryParam;

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

  @Override
  public String name() {
    return address;
  }

  /**
   * Sets the event bus address.
   *
   * @param address The event bus address.
   */
  @UriAuthority
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

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
  @UriQueryParam("vertx")
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Returns the Vert.x instance.
   *
   * @return The Vert.x instance.
   */
  public Vertx getVertx() {
    return vertx;
  }

  /**
   * Sets the Vert.x instance, returning the protocol for method chaining.
   *
   * @param vertx The Vert.x instance.
   * @return The event bus protocol.
   */
  public EventBusProtocol withVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Sets the Vert.x host.
   *
   * @param host The Vert.x host.
   */
  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the Vert.x host.
   *
   * @return The Vert.x host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the Vert.x host, returning the event bus protocol for method chaining.
   *
   * @param host The Vert.x host.
   * @return The event bus protocol.
   */
  public EventBusProtocol withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the Vert.x port.
   *
   * @param port The Vert.x port.
   */
  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the Vert.x port.
   *
   * @return The Vert.x port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the Vert.x port, returning the protocol for method chaining.
   *
   * @param port The Vert.x port.
   * @return The event bus protocol.
   */
  public EventBusProtocol withPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Creates a Vert.x instance.
   */
  private Vertx createVertx() {
    final CountDownLatch latch = new CountDownLatch(1);
    VertxOptions options = VertxOptions.options();
    options.setClusterPort(port);
    options.setClusterHost(host);
    Vertx.vertxAsync(options, (result) -> {
      vertx = result.result();
      latch.countDown();
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new ProtocolException(e);
    }
    return vertx;
  }

  @Override
  public synchronized ProtocolServer createServer() {
    if (vertx != null) {
      return new EventBusProtocolServer(address, vertx);
    } else {
      return new EventBusProtocolServer(address, createVertx());
    }
  }

  @Override
  public synchronized ProtocolClient createClient() {
    if (vertx != null) {
      return new EventBusProtocolClient(address, vertx);
    } else {
      return new EventBusProtocolClient(address, createVertx());
    }
  }

}
