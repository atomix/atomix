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
 * Netty TCP protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpProtocol implements Protocol {
  private int threads = 1;
  private int sendBufferSize = 8 * 1024;
  private int receiveBufferSize = 32 * 1024;
  private boolean useSsl;
  private int soLinger = -1;
  private int trafficClass = -1;
  private int acceptBacklog = 1024;
  private int connectTimeout = 60000;

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
  public NettyTcpProtocol withThreads(int numThreads) {
    this.threads = numThreads;
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
  public NettyTcpProtocol withSendBufferSize(int bufferSize) {
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
  public NettyTcpProtocol withReceiveBufferSize(int bufferSize) {
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
  public NettyTcpProtocol withSsl(boolean useSsl) {
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
  public NettyTcpProtocol withSoLinger(int soLinger) {
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
  public NettyTcpProtocol withTrafficClass(int trafficClass) {
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
  public NettyTcpProtocol withAcceptBacklog(int backlog) {
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
  public NettyTcpProtocol withConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  @Override
  public ProtocolServer createServer(URI endpoint) {
    return new NettyTcpServer(this, endpoint);
  }

  @Override
  public ProtocolClient createClient(URI endpoint) {
    return new NettyTcpClient(this, endpoint);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
