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

import java.net.URI;

import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.spi.protocol.ProtocolClient;
import net.kuujo.copycat.spi.protocol.ProtocolServer;

/**
 * TCP based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocol implements Protocol {
  private int sendBufferSize = 8 * 1024;
  private int receiveBufferSize = 32 * 1024;
  private boolean useSsl;
  private String keyStorePath;
  private String keyStorePassword;
  private String trustStorePath;
  private String trustStorePassword;
  private int acceptBacklog = 1024;
  private int connectTimeout = 60000;

  public VertxTcpProtocol() {
  }

  /**
   * Sets the send buffer size.
   *
   * @param bufferSize The send buffer size.
   */
  public void setSendBufferSize(int bufferSize) {
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
   */
  public VertxTcpProtocol withSendBufferSize(int bufferSize) {
    this.sendBufferSize = bufferSize;
    return this;
  }

  /**
   * Sets the receive buffer size.
   *
   * @param bufferSize The receive buffer size.
   */
  public void setReceiveBufferSize(int bufferSize) {
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
   */
  public VertxTcpProtocol withReceiveBufferSize(int bufferSize) {
    this.receiveBufferSize = bufferSize;
    return this;
  }

  /**
   * Sets whether to use SSL encryption.
   *
   * @param useSsl Whether to use SSL encryption.
   */
  public void setSsl(boolean useSsl) {
    this.useSsl = useSsl;
  }

  /**
   * Returns a boolean indicating whether SSL encryption is enabled.
   *
   * @return Indicates whether SSL encryption is enabled.
   */
  public boolean isSsl() {
    return useSsl;
  }

  /**
   * Sets whether to use SSL encryption, returning the protocol for method chaining.
   *
   * @param useSsl Whether to use SSL encryption.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withSsl(boolean useSsl) {
    this.useSsl = useSsl;
    return this;
  }

  /**
   * Sets the key store path.
   *
   * @param keyStorePath The key store path.
   */
  public void setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
  }

  /**
   * Returns the key store path.
   *
   * @return The key store path.
   */
  public String getKeyStorePath() {
    return keyStorePath;
  }

  /**
   * Sets the key store path, returning the protocol for method chaining.
   *
   * @param keyStorePath The key store path.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
    return this;
  }

  /**
   * Sets the key store password.
   *
   * @param keyStorePassword The key store password.
   */
  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  /**
   * Returns the key store password.
   *
   * @return The key store password.
   */
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  /**
   * Sets the key store password, returning the protocol for method chaining.
   *
   * @param keyStorePassword The key store password.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
    return this;
  }

  /**
   * Sets the trust store path.
   *
   * @param path The trust store path.
   */
  public void setTrustStorePath(String path) {
    this.trustStorePath = path;
  }

  /**
   * Returns the trust store path.
   *
   * @return The trust store path.
   */
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /**
   * Sets the trust store path, returning the protocol for method chaining.
   *
   * @param path The trust store path.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withTrustStorePath(String path) {
    this.trustStorePath = path;
    return this;
  }

  /**
   * Sets the trust store password.
   *
   * @param password The trust store password.
   */
  public void setTrustStorePassword(String password) {
    this.trustStorePassword = password;
  }

  /**
   * Returns the trust store password.
   *
   * @return The trust store password.
   */
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * Sets the trust store password, returning the protocol for method chaining.
   *
   * @param password The trust store password.
   * @return The TCP protocol.
   */
  public VertxTcpProtocol withTrustStorePassword(String password) {
    this.trustStorePassword = password;
    return this;
  }

  /**
   * Sets the accept backlog.
   *
   * @param backlog The accept backlog.
   */
  public void setAcceptBacklog(int backlog) {
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
   */
  public VertxTcpProtocol withAcceptBacklog(int backlog) {
    this.acceptBacklog = backlog;
    return this;
  }

  /**
   * Sets the connection timeout.
   *
   * @param connectTimeout The connection timeout.
   */
  public void setConnectTimeout(int connectTimeout) {
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
   */
  public VertxTcpProtocol withConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  @Override
  public ProtocolServer createServer(URI endpoint) {
    return new VertxTcpServer(endpoint.getHost(), endpoint.getPort(), this);
  }

  @Override
  public ProtocolClient createClient(URI endpoint) {
    return new VertxTcpClient(endpoint.getHost(), endpoint.getPort(), this);
  }
}
