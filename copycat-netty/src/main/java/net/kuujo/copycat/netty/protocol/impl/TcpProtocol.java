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
package net.kuujo.copycat.netty.protocol.impl;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriPort;

/**
 * Netty TCP protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocol implements Protocol {
  private String host = "localhost";
  private int port;
  private int threads = 1;
  private boolean tcpKeepAlive = true;
  private boolean tcpNoDelay;
  private boolean reuseAddress;
  private int sendBufferSize = 8 * 1024;
  private int receiveBufferSize = 32 * 1024;
  private boolean useSsl;
  private int soLinger = -1;
  private int trafficClass = -1;
  private int acceptBacklog = 1024;
  private int connectTimeout = 60000;

  public TcpProtocol() {
    this("localhost", 0);
  }

  public TcpProtocol(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public TcpProtocol(int port) {
    this("localhost", port);
  }

  /**
   * Sets the protocol host.
   *
   * @param host The TCP host.
   */
  @UriHost
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the protocol host.
   *
   * @return The protocol host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the protocol host, returning the protocol for method chaining.
   *
   * @param host The TCP host.
   * @return The TCP protocol.
   */
  public TcpProtocol withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the protocol port.
   *
   * @param port The TCP port.
   */
  @UriPort
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the protocol port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the protocol port, returning the protocol for method chaining.
   *
   * @param port The TCP port.
   * @return The TCP protocol.
   */
  public TcpProtocol withPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Sets the number of server threads to run.
   *
   * @param numThreads The number of server threads to run.
   */
  public void setThreads(int numThreads) {
    this.threads = numThreads;
  }

  /**
   * Returns the number of server threads to run.
   *
   * @return The number of server threads to run.
   */
  public int getThreads() {
    return threads;
  }

  /**
   * Sets the number of server threads to run, returning the protocol for method chaining.
   *
   * @param numThreads The number of server threads to run.
   * @return The TCP protocol.
   */
  public TcpProtocol withThreads(int numThreads) {
    this.threads = numThreads;
    return this;
  }

  /**
   * Sets whether to keep connections alive.
   *
   * @param keepAlive Whether to keep connections alive.
   */
  public void setKeepAlive(boolean keepAlive) {
    this.tcpKeepAlive = keepAlive;
  }

  /**
   * Returns a boolean indicating whether keepAlive is enabled.
   *
   * @return Whether keepAlive is enabled.
   */
  public boolean isKeepAlive() {
    return tcpKeepAlive;
  }

  /**
   * Sets whether to keep connections alive, returning the protocol for method chaining.
   *
   * @param keepAlive Whether to keep connections alive.
   * @return The TCP protocol.
   */
  public TcpProtocol withKeepAlive(boolean keepAlive) {
    this.tcpKeepAlive = keepAlive;
    return this;
  }

  /**
   * Sets the TCP no delay option.
   *
   * @param noDelay If <code>true</code>, turns of Nagle's algorithm.
   */
  public void setNoDelay(boolean noDelay) {
    this.tcpNoDelay = noDelay;
  }

  /**
   * Returns the TCP no delay option.
   *
   * @return Indicates whether Nagle's algorithm is disable.
   */
  public boolean isNoDelay() {
    return tcpNoDelay;
  }

  /**
   * Sets the TCP no delay option, returning the protocol for method chaining.
   *
   * @param noDelay If <code>true</code>, turns of Nagle's algorithm.
   * @return The TCP protocol.
   */
  public TcpProtocol withNoDelay(boolean noDelay) {
    this.tcpNoDelay = noDelay;
    return this;
  }

  /**
   * Sets the reuseAddres setting.
   *
   * @param reuseAddress Whether to reuseAddress.
   */
  public void setReuseAddress(boolean reuseAddress) {
    this.reuseAddress = reuseAddress;
  }

  /**
   * Returns the reuseAddress setting.
   *
   * @return Whether to reuseAddress.
   */
  public boolean isReuseAddress() {
    return reuseAddress;
  }

  /**
   * Sets the reuseAddress setting, returning the protocol for method chaining.
   *
   * @param reuseAddress Whether to reuseAddress.
   * @return The TCP protocol.
   */
  public TcpProtocol withReuseAddress(boolean reuseAddress) {
    this.reuseAddress = reuseAddress;
    return this;
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
  public TcpProtocol withSendBufferSize(int bufferSize) {
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
  public TcpProtocol withReceiveBufferSize(int bufferSize) {
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
  public TcpProtocol withSsl(boolean useSsl) {
    this.useSsl = useSsl;
    return this;
  }

  /**
   * Sets the TCP soLinger settings for connections.
   *
   * @param soLinger TCP soLinger settings for connections.
   */
  public void setSoLinger(int soLinger) {
    this.soLinger = soLinger;
  }

  /**
   * Returns TCP soLinger settings for connections.
   *
   * @return TCP soLinger settings for connections.
   */
  public int getSoLinger() {
    return soLinger;
  }

  /**
   * Sets TCP soLinger settings for connections, returning the protocol for method chaining.
   *
   * @param soLinger TCP soLinger settings for connections.
   * @return The TCP protocol.
   */
  public TcpProtocol withSoLinger(int soLinger) {
    this.soLinger = soLinger;
    return this;
  }

  /**
   * Sets the traffic class.
   *
   * @param trafficClass The traffic class.
   */
  public void setTrafficClass(int trafficClass) {
    this.trafficClass = trafficClass;
  }

  /**
   * Returns the traffic class.
   *
   * @return The traffic class.
   */
  public int getTrafficClass() {
    return trafficClass;
  }

  /**
   * Sets the traffic class, returning the protocol for method chaining.
   *
   * @param trafficClass The traffic class.
   * @return The TCP protocol.
   */
  public TcpProtocol withTrafficClass(int trafficClass) {
    this.trafficClass = trafficClass;
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
  public TcpProtocol withAcceptBacklog(int backlog) {
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
  public TcpProtocol withConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  @Override
  public ProtocolServer createServer() {
    return new TcpProtocolServer(this);
  }

  @Override
  public ProtocolClient createClient() {
    return new TcpProtocolClient(this);
  }

}
