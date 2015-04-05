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
package net.kuujo.copycat.vertx;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;

import java.net.URI;
import java.util.UUID;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocol implements Protocol {
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = -1;

  private static Vertx vertx;
  private String host = DEFAULT_HOST;
  private int port = DEFAULT_PORT;

  public VertxEventBusProtocol() {
  }

  public VertxEventBusProtocol(String host, int port) {
    setHost(host);
    setPort(port);
  }

  public VertxEventBusProtocol(Vertx vertx) {
    setVertx(vertx);
  }

  private VertxEventBusProtocol(VertxEventBusProtocol protocol) {
    this.host = protocol.host;
    this.port = protocol.port;
  }

  @Override
  public VertxEventBusProtocol copy() {
    return new VertxEventBusProtocol(this);
  }

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
  public void setVertx(Vertx vertx) {
    VertxEventBusProtocol.vertx = vertx;
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
    setVertx(vertx);
    return this;
  }

  /**
   * Sets the Vert.x host.
   *
   * @param host The Vert.x host.
   * @throws java.lang.NullPointerException If the host is {@code null}
   */
  public void setHost(String host) {
    if (host == null)
      throw new NullPointerException("host cannot be null");
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
    setHost(host);
    return this;
  }

  /**
   * Sets the Vert.x port.
   *
   * @param port The Vert.x port.
   */
  public void setPort(int port) {
    if (port < -1)
      throw new IllegalArgumentException("port must be positive");
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
    setPort(port);
    return this;
  }

  /**
   * Creates a new Vert.x instance.
   */
  private Vertx createVertx() {
    String host = getHost();
    int port = getPort();
    return host != null && !host.isEmpty() ? VertxFactory.newVertx(port, host) : VertxFactory.newVertx();
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    Vertx vertx = getVertx();
    if (vertx != null) {
      return new VertxEventBusProtocolServer(uri != null ? uri.getAuthority() : UUID.randomUUID().toString(), vertx);
    } else {
      return new VertxEventBusProtocolServer(uri != null ? uri.getAuthority() : UUID.randomUUID().toString(), createVertx());
    }
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    Vertx vertx = getVertx();
    if (vertx != null) {
      return new VertxEventBusProtocolClient(uri.getAuthority(), vertx);
    } else {
      return new VertxEventBusProtocolClient(uri.getAuthority(), createVertx());
    }
  }

}
