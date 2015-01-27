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
package net.kuujo.copycat.netty;

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.protocol.AbstractProtocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.net.URI;
import java.util.Map;

/**
 * Netty TCP protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpProtocol extends AbstractProtocol {
  private static final String NETTY_EVENT_LOOP_THREADS = "event-loop.threads";
  private static final String NETTY_SEND_BUFFER_SIZE = "send.buffer.size";
  private static final String NETTY_RECEIVE_BUFFER_SIZE = "receive.buffer.size";
  private static final String NETTY_USE_SSL = "ssl.enabled";
  private static final String NETTY_SO_LINGER = "solinger";
  private static final String NETTY_TRAFFIC_CLASS = "traffic.class";
  private static final String NETTY_ACCEPT_BACKLOG = "accept.backlog";
  private static final String NETTY_CONNECT_TIMEOUT = "connect.timeout";

  private static final String CONFIGURATION = "tcp";
  private static final String DEFAULT_CONFIGURATION = "tcp-defaults";

  public NettyTcpProtocol() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public NettyTcpProtocol(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public NettyTcpProtocol(String resource) {
    super(addResources(new String[]{resource}, CONFIGURATION, DEFAULT_CONFIGURATION));
  }

  private NettyTcpProtocol(NettyTcpProtocol protocol) {
    super(protocol);
  }

  @Override
  public Configurable copy() {
    return new NettyTcpProtocol(this);
  }

  /**
   * Sets the number of server threads to run.
   *
   * @param numThreads The number of server threads to run.
   * @throws java.lang.IllegalArgumentException If the number of threads is not positive
   */
  public void setThreads(int numThreads) {
    this.config = config.withValue(NETTY_EVENT_LOOP_THREADS, ConfigValueFactory.fromAnyRef(Assert.arg(numThreads, numThreads > 0, "number of threads must be positive")));
  }

  /**
   * Returns the number of server threads to run.
   *
   * @return The number of server threads to run.
   */
  public int getThreads() {
    return config.getInt(NETTY_EVENT_LOOP_THREADS);
  }

  /**
   * Sets the number of server threads to run, returning the protocol for method chaining.
   *
   * @param numThreads The number of server threads to run.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the number of threads is not positive
   */
  public NettyTcpProtocol withThreads(int numThreads) {
    setThreads(numThreads);
    return this;
  }

  /**
   * Sets the send buffer size.
   *
   * @param bufferSize The send buffer size.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public void setSendBufferSize(int bufferSize) {
    this.config = config.withValue(NETTY_SEND_BUFFER_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive")));
  }

  /**
   * Returns the send buffer size.
   *
   * @return The send buffer size.
   */
  public int getSendBufferSize() {
    return config.getInt(NETTY_SEND_BUFFER_SIZE);
  }

  /**
   * Sets the send buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The send buffer size.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public NettyTcpProtocol withSendBufferSize(int bufferSize) {
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
    this.config = config.withValue(NETTY_RECEIVE_BUFFER_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive")));
  }

  /**
   * Returns the receive buffer size.
   *
   * @return The receive buffer size.
   */
  public int getReceiveBufferSize() {
    return config.getInt(NETTY_RECEIVE_BUFFER_SIZE);
  }

  /**
   * Sets the receive buffer size, returning the protocol for method chaining.
   *
   * @param bufferSize The receive buffer size.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public NettyTcpProtocol withReceiveBufferSize(int bufferSize) {
    setReceiveBufferSize(bufferSize);
    return this;
  }

  /**
   * Sets whether to use SSL encryption.
   *
   * @param useSsl Whether to use SSL encryption.
   */
  public void setSsl(boolean useSsl) {
    this.config = config.withValue(NETTY_USE_SSL, ConfigValueFactory.fromAnyRef(useSsl));
  }

  /**
   * Returns a boolean indicating whether SSL encryption is enabled.
   *
   * @return Indicates whether SSL encryption is enabled.
   */
  public boolean isSsl() {
    return config.getBoolean(NETTY_USE_SSL);
  }

  /**
   * Sets whether to use SSL encryption, returning the protocol for method chaining.
   *
   * @param useSsl Whether to use SSL encryption.
   * @return The TCP protocol.
   */
  public NettyTcpProtocol withSsl(boolean useSsl) {
    setSsl(useSsl);
    return this;
  }

  /**
   * Sets the TCP soLinger settings for connections.
   *
   * @param soLinger TCP soLinger settings for connections.
   */
  public void setSoLinger(int soLinger) {
    this.config = config.withValue(NETTY_SO_LINGER, ConfigValueFactory.fromAnyRef(soLinger));
  }

  /**
   * Returns TCP soLinger settings for connections.
   *
   * @return TCP soLinger settings for connections.
   */
  public int getSoLinger() {
    return config.getInt(NETTY_SO_LINGER);
  }

  /**
   * Sets TCP soLinger settings for connections, returning the protocol for method chaining.
   *
   * @param soLinger TCP soLinger settings for connections.
   * @return The TCP protocol.
   */
  public NettyTcpProtocol withSoLinger(int soLinger) {
    setSoLinger(soLinger);
    return this;
  }

  /**
   * Sets the traffic class.
   *
   * @param trafficClass The traffic class.
   */
  public void setTrafficClass(int trafficClass) {
    this.config = config.withValue(NETTY_TRAFFIC_CLASS, ConfigValueFactory.fromAnyRef(trafficClass));
  }

  /**
   * Returns the traffic class.
   *
   * @return The traffic class.
   */
  public int getTrafficClass() {
    return config.getInt(NETTY_TRAFFIC_CLASS);
  }

  /**
   * Sets the traffic class, returning the protocol for method chaining.
   *
   * @param trafficClass The traffic class.
   * @return The TCP protocol.
   */
  public NettyTcpProtocol withTrafficClass(int trafficClass) {
    setTrafficClass(trafficClass);
    return this;
  }

  /**
   * Sets the accept backlog.
   *
   * @param backlog The accept backlog.
   * @throws java.lang.IllegalArgumentException If the accept backlog is not positive
   */
  public void setAcceptBacklog(int backlog) {
    this.config = config.withValue(NETTY_ACCEPT_BACKLOG, ConfigValueFactory.fromAnyRef(Assert.arg(backlog, backlog > 0, "accept backlog must be positive")));
  }

  /**
   * Returns the accept backlog.
   *
   * @return The accept backlog.
   */
  public int getAcceptBacklog() {
    return config.getInt(NETTY_ACCEPT_BACKLOG);
  }

  /**
   * Sets the accept backlog, returning the protocol for method chaining.
   *
   * @param backlog The accept backlog.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the accept backlog is not positive
   */
  public NettyTcpProtocol withAcceptBacklog(int backlog) {
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
    this.config = config.withValue(NETTY_CONNECT_TIMEOUT, ConfigValueFactory.fromAnyRef(Assert.arg(connectTimeout, connectTimeout > 0, "connect timeout must be positive")));
  }

  /**
   * Returns the connection timeout.
   *
   * @return The connection timeout.
   */
  public int getConnectTimeout() {
    return config.getInt(NETTY_CONNECT_TIMEOUT);
  }

  /**
   * Sets the connection timeout, returning the protocol for method chaining.
   *
   * @param connectTimeout The connection timeout.
   * @return The TCP protocol.
   * @throws java.lang.IllegalArgumentException If the connect timeout is not positive
   */
  public NettyTcpProtocol withConnectTimeout(int connectTimeout) {
    setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public ProtocolServer createServer(URI uri) {
    return new NettyTcpProtocolServer(uri.getHost(), uri.getPort(), this);
  }

  @Override
  public ProtocolClient createClient(URI uri) {
    return new NettyTcpProtocolClient(uri.getHost(), uri.getPort(), this);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
