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

import net.kuujo.copycat.internal.util.Assert;

import java.net.URI;

/**
 * TCP based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocol extends AbstractProtocol {
  public static final String VERTX_TCP_SEND_BUFFER_SIZE = "send-buffer-size";
  public static final String VERTX_TCP_RECEIVE_BUFFER_SIZE = "receive-buffer-size";
  public static final String VERTX_TCP_USE_SSL = "ssl";
  public static final String VERTX_TCP_KEY_STORE_PATH = "key-store-path";
  public static final String VERTX_TCP_KEY_STORE_PASSWORD = "key-store-password";
  public static final String VERTX_TCP_TRUST_STORE_PATH = "trust-store-path";
  public static final String VERTX_TCP_TRUST_STORE_PASSWORD = "trust-store-password";
  public static final String VERTX_TCP_ACCEPT_BACKLOG = "accept-backlog";
  public static final String VERTX_TCP_CONNECT_TIMEOUT = "connect-timeout";

  private int DEFAULT_VERTX_TCP_SEND_BUFFER_SIZE = 8 * 1024;
  private int DEFAULT_VERTX_TCP_RECEIVE_BUFFER_SIZE = 32 * 1024;
  private boolean DEFAULT_VERTX_TCP_USE_SSL;
  private String DEFAULT_VERTX_TCP_KEY_STORE_PATH;
  private String DEFAULT_VERTX_TCP_KEY_STORE_PASSWORD;
  private String DEFAULT_VERTX_TCP_TRUST_STORE_PATH;
  private String DEFAULT_VERTX_TCP_TRUST_STORE_PASSWORD;
  private int DEFAULT_VERTX_TCP_ACCEPT_BACKLOG = 1024;
  private int DEFAULT_VERTX_TCP_CONNECT_TIMEOUT = 60000;

  public VertxTcpProtocol() {
  }

  /**
   * Sets the send buffer size.
   *
   * @param bufferSize The send buffer size.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public void setSendBufferSize(int bufferSize) {
    put(VERTX_TCP_SEND_BUFFER_SIZE, Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive"));
  }

  /**
   * Returns the send buffer size.
   *
   * @return The send buffer size.
   */
  public int getSendBufferSize() {
    return get(VERTX_TCP_SEND_BUFFER_SIZE, DEFAULT_VERTX_TCP_SEND_BUFFER_SIZE);
  }

  /**
   * Sets the send buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The send buffer size.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public VertxTcpProtocol withSendBufferSize(int bufferSize) {
    setSendBufferSize(bufferSize);
    return this;
  }

  /**
   * Sets the receive buffer size.
   *
   * @param bufferSize The receive buffer size.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public void setReceiveBufferSize(int bufferSize) {
    put(VERTX_TCP_RECEIVE_BUFFER_SIZE, Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive"));
  }

  /**
   * Returns the receive buffer size.
   *
   * @return The receive buffer size.
   */
  public int getReceiveBufferSize() {
    return get(VERTX_TCP_RECEIVE_BUFFER_SIZE, DEFAULT_VERTX_TCP_RECEIVE_BUFFER_SIZE);
  }

  /**
   * Sets the receive buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The receive buffer size.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
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
    put(VERTX_TCP_USE_SSL, useSsl);
  }

  /**
   * Returns a boolean indicating whether SSL encryption is enabled.
   *
   * @return Indicates whether SSL encryption is enabled.
   */
  public boolean isSsl() {
    return get(VERTX_TCP_USE_SSL, DEFAULT_VERTX_TCP_USE_SSL);
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
   * Sets the key store path.
   *
   * @param keyStorePath The key store path.
   * @throws java.lang.NullPointerException If the key store path is {@code null}
   */
  public void setKeyStorePath(String keyStorePath) {
    put(VERTX_TCP_KEY_STORE_PATH, Assert.isNotNull(keyStorePath, "keyStorePath"));
  }

  /**
   * Returns the key store path.
   *
   * @return The key store path.
   */
  public String getKeyStorePath() {
    return get(VERTX_TCP_KEY_STORE_PATH, DEFAULT_VERTX_TCP_KEY_STORE_PATH);
  }

  /**
   * Sets the key store path, returning the protocol for method chaining.
   *
   * @param keyStorePath The key store path.
   * @return The TCP protocol.
   * @throws java.lang.NullPointerException If the key store path is {@code null}
   */
  public VertxTcpProtocol withKeyStorePath(String keyStorePath) {
    setKeyStorePath(keyStorePath);
    return this;
  }

  /**
   * Sets the key store password.
   *
   * @param keyStorePassword The key store password.
   * @throws java.lang.NullPointerException If the key store password is {@code null}
   */
  public void setKeyStorePassword(String keyStorePassword) {
    put(VERTX_TCP_KEY_STORE_PASSWORD, Assert.isNotNull(keyStorePassword, "keyStorePassword"));
  }

  /**
   * Returns the key store password.
   *
   * @return The key store password.
   */
  public String getKeyStorePassword() {
    return get(VERTX_TCP_KEY_STORE_PASSWORD, DEFAULT_VERTX_TCP_KEY_STORE_PASSWORD);
  }

  /**
   * Sets the key store password, returning the protocol for method chaining.
   *
   * @param keyStorePassword The key store password.
   * @return The TCP protocol.
   * @throws java.lang.NullPointerException If the key store password is {@code null}
   */
  public VertxTcpProtocol withKeyStorePassword(String keyStorePassword) {
    setKeyStorePassword(keyStorePassword);
    return this;
  }

  /**
   * Sets the trust store path.
   *
   * @param path The trust store path.
   * @throws java.lang.NullPointerException If the trust store path is {@code null}
   */
  public void setTrustStorePath(String path) {
    put(VERTX_TCP_TRUST_STORE_PATH, Assert.isNotNull(path, "path"));
  }

  /**
   * Returns the trust store path.
   *
   * @return The trust store path.
   */
  public String getTrustStorePath() {
    return get(VERTX_TCP_TRUST_STORE_PATH, DEFAULT_VERTX_TCP_TRUST_STORE_PATH);
  }

  /**
   * Sets the trust store path, returning the protocol for method chaining.
   *
   * @param path The trust store path.
   * @return The TCP protocol.
   * @throws java.lang.NullPointerException If the trust store path is {@code null}
   */
  public VertxTcpProtocol withTrustStorePath(String path) {
    setTrustStorePath(path);
    return this;
  }

  /**
   * Sets the trust store password.
   *
   * @param password The trust store password.
   * @throws java.lang.NullPointerException If the trust store password is {@code null}
   */
  public void setTrustStorePassword(String password) {
    put(VERTX_TCP_TRUST_STORE_PASSWORD, Assert.isNotNull(password, "password"));
  }

  /**
   * Returns the trust store password.
   *
   * @return The trust store password.
   */
  public String getTrustStorePassword() {
    return get(VERTX_TCP_TRUST_STORE_PASSWORD, DEFAULT_VERTX_TCP_TRUST_STORE_PASSWORD);
  }

  /**
   * Sets the trust store password, returning the protocol for method chaining.
   *
   * @param password The trust store password.
   * @return The TCP protocol.
   * @throws java.lang.NullPointerException If the trust store password is {@code null}
   */
  public VertxTcpProtocol withTrustStorePassword(String password) {
    setTrustStorePassword(password);
    return this;
  }

  /**
   * Sets the accept backlog.
   *
   * @param backlog The accept backlog.
   * @throws java.lang.IllegalArgumentException If the accept backlog is not positive
   */
  public void setAcceptBacklog(int backlog) {
    put(VERTX_TCP_ACCEPT_BACKLOG, Assert.arg(backlog, backlog > -1, "backlog must be positive"));
  }

  /**
   * Returns the accept backlog.
   *
   * @return The accept backlog.
   */
  public int getAcceptBacklog() {
    return get(VERTX_TCP_ACCEPT_BACKLOG, DEFAULT_VERTX_TCP_ACCEPT_BACKLOG);
  }

  /**
   * Sets the accept backlog, returning the protocol for method chaining.
   *
   * @param backlog The accept backlog.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the accept backlog is not positive
   */
  public VertxTcpProtocol withAcceptBacklog(int backlog) {
    setAcceptBacklog(backlog);
    return this;
  }

  /**
   * Sets the connection timeout.
   *
   * @param connectTimeout The connection timeout.
   * @throws java.lang.IllegalArgumentException If the connect timeout is not positive
   */
  public void setConnectTimeout(int connectTimeout) {
    put(VERTX_TCP_CONNECT_TIMEOUT, Assert.arg(connectTimeout, connectTimeout > 0, "connect timeout must be greater than zero"));
  }

  /**
   * Returns the connection timeout.
   *
   * @return The connection timeout.
   */
  public int getConnectTimeout() {
    return get(VERTX_TCP_CONNECT_TIMEOUT, DEFAULT_VERTX_TCP_CONNECT_TIMEOUT);
  }

  /**
   * Sets the connection timeout, returning the protocol for method chaining.
   *
   * @param connectTimeout The connection timeout.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the connect timeout is not positive
   */
  public VertxTcpProtocol withConnectTimeout(int connectTimeout) {
    setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    return new VertxTcpProtocolServer(uri.getHost(), uri.getPort(), this);
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    return new VertxTcpProtocolClient(uri.getHost(), uri.getPort(), this);
  }

}
