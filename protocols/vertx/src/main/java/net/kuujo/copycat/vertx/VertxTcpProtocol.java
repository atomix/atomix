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

import java.net.URI;
import java.util.Map;

/**
 * TCP based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocol extends AbstractProtocol {
  private static final String VERTX_TCP_SEND_BUFFER_SIZE = "send.buffer.size";
  private static final String VERTX_TCP_RECEIVE_BUFFER_SIZE = "receive.buffer.size";
  private static final String VERTX_TCP_USE_SSL = "ssl.enabled";
  private static final String VERTX_TCP_KEY_STORE_PATH = "ssl.key-store.path";
  private static final String VERTX_TCP_KEY_STORE_PASSWORD = "ssl.key-store.password";
  private static final String VERTX_TCP_TRUST_STORE_PATH = "ssl.trust-store.path";
  private static final String VERTX_TCP_TRUST_STORE_PASSWORD = "ssl.trust-store.password";
  private static final String VERTX_TCP_CLIENT_TRUST_ALL = "ssl.trust-all";
  private static final String VERTX_TCP_CLIENT_AUTH_REQUIRED = "ssl.auth-required";
  private static final String VERTX_TCP_ACCEPT_BACKLOG = "accept.backlog";
  private static final String VERTX_TCP_CONNECT_TIMEOUT = "connect.timeout";

  private static final String CONFIGURATION = "tcp";
  private static final String DEFAULT_CONFIGURATION = "tcp-defaults";

  private static Vertx vertx;

  public VertxTcpProtocol() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxTcpProtocol(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxTcpProtocol(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public VertxTcpProtocol(Vertx vertx) {
    this();
    setVertx(vertx);
  }

  @Override
  public VertxTcpProtocol copy() {
    return (VertxTcpProtocol) super.copy();
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
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public void setSendBufferSize(int bufferSize) {
    this.config = config.withValue(VERTX_TCP_SEND_BUFFER_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive")));
  }

  /**
   * Returns the send buffer size.
   *
   * @return The send buffer size.
   */
  public int getSendBufferSize() {
    return config.getInt(VERTX_TCP_SEND_BUFFER_SIZE);
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
    this.config = config.withValue(VERTX_TCP_RECEIVE_BUFFER_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive")));
  }

  /**
   * Returns the receive buffer size.
   *
   * @return The receive buffer size.
   */
  public int getReceiveBufferSize() {
    return config.getInt(VERTX_TCP_RECEIVE_BUFFER_SIZE);
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
    this.config = config.withValue(VERTX_TCP_USE_SSL, ConfigValueFactory.fromAnyRef(useSsl));
  }

  /**
   * Returns a boolean indicating whether SSL encryption is enabled.
   *
   * @return Indicates whether SSL encryption is enabled.
   */
  public boolean isSsl() {
    return config.getBoolean(VERTX_TCP_USE_SSL);
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
    this.config = config.withValue(VERTX_TCP_KEY_STORE_PATH, ConfigValueFactory.fromAnyRef(Assert.notNull(keyStorePath, "keyStorePath")));
  }

  /**
   * Returns the key store path.
   *
   * @return The key store path.
   */
  public String getKeyStorePath() {
    return config.hasPath(VERTX_TCP_KEY_STORE_PATH) ? config.getString(VERTX_TCP_KEY_STORE_PATH) : null;
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
    this.config = config.withValue(VERTX_TCP_KEY_STORE_PASSWORD, ConfigValueFactory.fromAnyRef(Assert.notNull(keyStorePassword, "keyStorePassword")));
  }

  /**
   * Returns the key store password.
   *
   * @return The key store password.
   */
  public String getKeyStorePassword() {
    return config.hasPath(VERTX_TCP_KEY_STORE_PASSWORD) ? config.getString(VERTX_TCP_KEY_STORE_PASSWORD) : null;
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
    this.config = config.withValue(VERTX_TCP_TRUST_STORE_PATH, ConfigValueFactory.fromAnyRef(Assert.notNull(path, "path")));
  }

  /**
   * Returns the trust store path.
   *
   * @return The trust store path.
   */
  public String getTrustStorePath() {
    return config.hasPath(VERTX_TCP_TRUST_STORE_PATH) ? config.getString(VERTX_TCP_TRUST_STORE_PATH) : null;
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
    this.config = config.withValue(VERTX_TCP_TRUST_STORE_PASSWORD, ConfigValueFactory.fromAnyRef(Assert.notNull(password, "password")));
  }

  /**
   * Returns the trust store password.
   *
   * @return The trust store password.
   */
  public String getTrustStorePassword() {
    return config.hasPath(VERTX_TCP_TRUST_STORE_PASSWORD) ? config.getString(VERTX_TCP_TRUST_STORE_PASSWORD) : null;
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
   * Sets whether to trust all server certs.
   *
   * @param trustAll Whether to trust all server certs.
   */
  public void setClientTrustAll(boolean trustAll) {
    this.config = config.withValue(VERTX_TCP_CLIENT_TRUST_ALL, ConfigValueFactory.fromAnyRef(trustAll));
  }

  /**
   * Returns whether to trust all server certs.
   *
   * @return Whether to trust all server certs.
   */
  public boolean isClientTrustAll() {
    return config.getBoolean(VERTX_TCP_CLIENT_TRUST_ALL);
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
    this.config = config.withValue(VERTX_TCP_CLIENT_AUTH_REQUIRED, ConfigValueFactory.fromAnyRef(required));
  }

  /**
   * Returns whether client authentication is required.
   *
   * @return Whether client authentication is required.
   */
  public boolean isClientAuthRequired() {
    return config.getBoolean(VERTX_TCP_CLIENT_AUTH_REQUIRED);
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
   * @throws java.lang.IllegalArgumentException If the accept backlog is not positive
   */
  public void setAcceptBacklog(int backlog) {
    this.config = config.withValue(VERTX_TCP_ACCEPT_BACKLOG, ConfigValueFactory.fromAnyRef(Assert.arg(backlog, backlog > -1, "backlog must be positive")));
  }

  /**
   * Returns the accept backlog.
   *
   * @return The accept backlog.
   */
  public int getAcceptBacklog() {
    return config.getInt(VERTX_TCP_ACCEPT_BACKLOG);
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
    this.config = config.withValue(VERTX_TCP_CONNECT_TIMEOUT, ConfigValueFactory.fromAnyRef(Assert.arg(connectTimeout, connectTimeout > 0, "connect timeout must be greater than zero")));
  }

  /**
   * Returns the connection timeout.
   *
   * @return The connection timeout.
   */
  public int getConnectTimeout() {
    return config.getInt(VERTX_TCP_CONNECT_TIMEOUT);
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
