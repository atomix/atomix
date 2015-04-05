/*
 * Copyright 2015 the original author or authors.
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

import io.vertx.core.Vertx;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;

import java.net.URI;

/**
 * TCP based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocol implements Protocol {
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = -1;
  private static final int DEFAULT_SEND_BUFFER_SIZE = 8192;
  private static final int DEFAULT_RECEIVE_BUFFER_SIZE = 32768;
  private static final int DEFAULT_ACCEPT_BACKLOG = 1024;
  private static final int DEFAULT_CONNECT_TIMEOUT = 60000;

  private static Vertx vertx;
  private String host = DEFAULT_HOST;
  private int port = DEFAULT_PORT;
  private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
  private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
  private boolean sslEnabled = false;
  private boolean trustAll = false;
  private boolean authRequired = false;
  private int acceptBacklog = DEFAULT_ACCEPT_BACKLOG;
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

  public VertxTcpProtocol() {
  }

  public VertxTcpProtocol(Vertx vertx) {
    setVertx(vertx);
  }

  private VertxTcpProtocol(VertxTcpProtocol protocol) {
    this.host = protocol.host;
    this.port = protocol.port;
    this.sendBufferSize = protocol.sendBufferSize;
    this.receiveBufferSize = protocol.receiveBufferSize;
    this.sslEnabled = protocol.sslEnabled;
    this.trustAll = protocol.trustAll;
    this.authRequired = protocol.authRequired;
    this.acceptBacklog = protocol.acceptBacklog;
    this.connectTimeout = protocol.connectTimeout;
  }

  @Override
  public VertxTcpProtocol copy() {
    return new VertxTcpProtocol(this);
  }

  /**
   * Sets the Vert.x instance.
   *
   * @param vertx The Vert.x instance.
   */
  public void setVertx(Vertx vertx) {
    VertxTcpProtocol.vertx = vertx;
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
   * Sets the Vert.x instance, returning the configuration for method chaining.
   *
   * @param vertx The Vert.x instance.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withVertx(Vertx vertx) {
    setVertx(vertx);
    return this;
  }

  /**
   * Sets the send buffer size.
   *
   * @param bufferSize The send buffer size.
   * @throws IllegalArgumentException If the buffer size is not positive
   */
  public void setSendBufferSize(int bufferSize) {
    if (bufferSize <= 0)
      throw new IllegalArgumentException("bufferSize must be positive");
    this.sendBufferSize = bufferSize;
  }

  /**
   * Returns the send buffer size.
   *
   * @return The send buffer size.
   */
  public int getSendBufferSize() {
    return sendBufferSize;
  }

  /**
   * Sets the send buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The send buffer size.
   * @return The TCP protocol.
   * @throws IllegalArgumentException If the buffer size is not positive
   */
  public VertxTcpProtocol withSendBufferSize(int bufferSize) {
    setSendBufferSize(bufferSize);
    return this;
  }

  /**
   * Sets the receive buffer size.
   *
   * @param bufferSize The receive buffer size.
   * @throws IllegalArgumentException If the buffer size is not positive
   */
  public void setReceiveBufferSize(int bufferSize) {
    if (bufferSize <= 0)
      throw new IllegalArgumentException("bufferSize must be positive");
    this.receiveBufferSize = bufferSize;
  }

  /**
   * Returns the receive buffer size.
   *
   * @return The receive buffer size.
   */
  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  /**
   * Sets the receive buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The receive buffer size.
   * @return The TCP protocol.
   * @throws IllegalArgumentException If the buffer size is not positive
   */
  public VertxTcpProtocol withReceiveBufferSize(int bufferSize) {
    setReceiveBufferSize(bufferSize);
    return this;
  }

  /**
   * Sets whether to use SSL encryption.
   *
   * @param useSsl Whether to use SSL encryption.
   */
  public void setSsl(boolean useSsl) {
    this.sslEnabled = useSsl;
  }

  /**
   * Returns a boolean indicating whether SSL encryption is enabled.
   *
   * @return Indicates whether SSL encryption is enabled.
   */
  public boolean isSsl() {
    return sslEnabled;
  }

  /**
   * Sets whether to use SSL encryption, returning the protocol for method chaining.
   *
   * @param useSsl Whether to use SSL encryption.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withSsl(boolean useSsl) {
    setSsl(useSsl);
    return this;
  }

  /**
   * Sets whether to trust all server certs.
   *
   * @param trustAll Whether to trust all server certs.
   */
  public void setClientTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
  }

  /**
   * Returns whether to trust all server certs.
   *
   * @return Whether to trust all server certs.
   */
  public boolean isClientTrustAll() {
    return trustAll;
  }

  /**
   * Sets whether to trust all server certs, returning the protocol for method chaining.
   *
   * @param trustAll Whether to trust all server certs.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withClientTrustAll(boolean trustAll) {
    setClientTrustAll(trustAll);
    return this;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   */
  public void setClientAuthRequired(boolean required) {
    this.authRequired = required;
  }

  /**
   * Returns whether client authentication is required.
   *
   * @return Whether client authentication is required.
   */
  public boolean isClientAuthRequired() {
    return authRequired;
  }

  /**
   * Sets whether client authentication is required.
   *
   * @param required Whether client authentication is required.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withClientAuthRequired(boolean required) {
    setClientAuthRequired(required);
    return this;
  }

  /**
   * Sets the accept backlog.
   *
   * @param backlog The accept backlog.
   * @throws IllegalArgumentException If the accept backlog is not positive
   */
  public void setAcceptBacklog(int backlog) {
    if (backlog <= -1)
      throw new IllegalArgumentException("backlog must be positive or -1");
    this.acceptBacklog = backlog;
  }

  /**
   * Returns the accept backlog.
   *
   * @return The accept backlog.
   */
  public int getAcceptBacklog() {
    return acceptBacklog;
  }

  /**
   * Sets the accept backlog, returning the protocol for method chaining.
   *
   * @param backlog The accept backlog.
   * @return The TCP protocol.
   * @throws IllegalArgumentException If the accept backlog is not positive
   */
  public VertxTcpProtocol withAcceptBacklog(int backlog) {
    setAcceptBacklog(backlog);
    return this;
  }

  /**
   * Sets the connection timeout.
   *
   * @param connectTimeout The connection timeout.
   * @throws IllegalArgumentException If the connect timeout is not positive
   */
  public void setConnectTimeout(int connectTimeout) {
    if (connectTimeout <= 0)
      throw new IllegalArgumentException("connectTimeout must be positive");
    this.connectTimeout = connectTimeout;
  }

  /**
   * Returns the connection timeout.
   *
   * @return The connection timeout.
   */
  public int getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Sets the connection timeout, returning the protocol for method chaining.
   *
   * @param connectTimeout The connection timeout.
   * @return The TCP protocol.
   * @throws IllegalArgumentException If the connect timeout is not positive
   */
  public VertxTcpProtocol withConnectTimeout(int connectTimeout) {
    setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    if (uri != null) {
      return new VertxTcpProtocolServer(uri.getHost(), uri.getPort(), this);
    } else {
      return new VertxTcpProtocolServer(null, 0, this);
    }
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    return new VertxTcpProtocolClient(uri.getHost(), uri.getPort(), this);
  }

}
