package net.kuujo.copycat.netty.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInject;
import net.kuujo.copycat.uri.UriPort;

public class TcpProtocol implements Protocol {
  private String host = "localhost";
  private int port;
  private int threads = 1;
  private boolean tcpNoDelay;
  private int sendBufferSize = 4096;
  private int receiveBufferSize = 4096;
  private boolean useSsl;

  public TcpProtocol() {
    this("localhost", 0);
  }

  @UriInject
  public TcpProtocol(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
  }

  @UriInject
  public TcpProtocol(@UriHost String host) {
    this(host, 0);
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

  public void setThreads(int numThreads) {
    this.threads = numThreads;
  }

  public int getThreads() {
    return threads;
  }

  public TcpProtocol withThreads(int numThreads) {
    this.threads = numThreads;
    return this;
  }

  public void setTcpNoDelay(boolean noDelay) {
    this.tcpNoDelay = noDelay;
  }

  public boolean isTcpNoDelay() {
    return tcpNoDelay;
  }

  public TcpProtocol withTcpNoDelay(boolean noDelay) {
    this.tcpNoDelay = noDelay;
    return this;
  }

  public void setSendBufferSize(int bufferSize) {
    this.sendBufferSize = bufferSize;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public TcpProtocol withSendBufferSize(int bufferSize) {
    this.sendBufferSize = bufferSize;
    return this;
  }

  public void setReceiveBufferSize(int bufferSize) {
    this.receiveBufferSize = bufferSize;
  }

  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  public TcpProtocol withReceiveBufferSize(int bufferSize) {
    this.receiveBufferSize = bufferSize;
    return this;
  }

  public void setUseSsl(boolean useSsl) {
    this.useSsl = useSsl;
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public TcpProtocol withUseSsl(boolean useSsl) {
    this.useSsl = useSsl;
    return this;
  }

  @Override
  public void init(CopyCatContext context) {
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
