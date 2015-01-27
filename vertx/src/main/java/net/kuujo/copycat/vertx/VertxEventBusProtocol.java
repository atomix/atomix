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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.protocol.AbstractProtocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.util.internal.Assert;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;

import java.net.URI;
import java.util.Map;

/**
 * Vert.x event bus protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxEventBusProtocol extends AbstractProtocol {
  private static final String VERTX = "vertx";
  private static final String VERTX_HOST = "host";
  private static final String VERTX_PORT = "port";

  private static final String CONFIGURATION = "eventbus";
  private static final String DEFAULT_CONFIGURATION = "eventbus-defaults";

  public VertxEventBusProtocol() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxEventBusProtocol(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxEventBusProtocol(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxEventBusProtocol(String host, int port) {
    super();
    setHost(host);
    setPort(port);
  }

  public VertxEventBusProtocol(Vertx vertx) {
    setVertx(vertx);
  }

  @Override
  public VertxEventBusProtocol copy() {
    return (VertxEventBusProtocol) super.copy();
  }

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
  public void setVertx(Vertx vertx) {
    this.config = config.withValue(VERTX, ConfigValueFactory.fromAnyRef(Assert.isNotNull(vertx, "vertx")));
  }

  /**
   * Returns the Vert.x instance.
   *
   * @return The Vert.x instance.
   */
  public Vertx getVertx() {
    return config.hasPath(VERTX) ? (Vertx) config.getValue(VERTX) : null;
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
    this.config = config.withValue(VERTX_HOST, ConfigValueFactory.fromAnyRef(Assert.isNotNull(host, "host")));
  }

  /**
   * Returns the Vert.x host.
   *
   * @return The Vert.x host.
   */
  public String getHost() {
    return config.getString(VERTX_HOST);
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
    this.config = config.withValue(VERTX_PORT, ConfigValueFactory.fromAnyRef(Assert.arg(port, port > -1, "port must be positive")));
  }

  /**
   * Returns the Vert.x port.
   *
   * @return The Vert.x port.
   */
  public int getPort() {
    return config.getInt(VERTX_PORT);
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
    return host != null ? VertxFactory.newVertx(port, host) : VertxFactory.newVertx();
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    Vertx vertx = getVertx();
    if (vertx != null) {
      return new VertxEventBusProtocolServer(uri.getAuthority(), vertx);
    } else {
      return new VertxEventBusProtocolServer(uri.getAuthority(), createVertx());
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
