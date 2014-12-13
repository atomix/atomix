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
package net.kuujo.copycat.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import net.kuujo.copycat.spi.Protocol;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocol implements Protocol {
  private String host;
  private int port;
  private Vertx vertx;

  public VertxEventBusProtocol() {
  }

  public VertxEventBusProtocol(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public VertxEventBusProtocol(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
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
  public VertxEventBusProtocol withVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Sets the Vert.x host.
   *
   * @param host The Vert.x host.
   */
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
  public VertxEventBusProtocol withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the Vert.x port.
   *
   * @param port The Vert.x port.
   */
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
  public VertxEventBusProtocol withPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Creates a Vert.x instance.
   */
  private Vertx createVertx() {
    final CountDownLatch latch = new CountDownLatch(1);
    VertxOptions options = new VertxOptions();
    options.setClusterPort(port);
    options.setClusterHost(host);
    vertx = Vertx.vertx(options);
    return vertx;
  }

  @Override
  public synchronized ProtocolServer createServer(URI uri) {
    if (vertx != null) {
      return new VertxEventBusProtocolServer(uri.getAuthority(), vertx);
    } else {
      return new VertxEventBusProtocolServer(uri.getAuthority(), createVertx());
    }
  }

  @Override
  public synchronized ProtocolClient createClient(URI uri) {
    if (vertx != null) {
      return new VertxEventBusProtocolClient(uri.getAuthority(), vertx);
    } else {
      return new VertxEventBusProtocolClient(uri.getAuthority(), createVertx());
    }
  }

  @Override
  public String toString() {
    return String.format("EventBusProtocol[host=%s, port=%d]", host, port);
  }

}
