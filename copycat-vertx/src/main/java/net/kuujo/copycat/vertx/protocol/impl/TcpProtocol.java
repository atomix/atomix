package net.kuujo.copycat.vertx.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.uri.Optional;
import net.kuujo.copycat.uri.UriArgument;
import net.kuujo.copycat.uri.UriHost;
import net.kuujo.copycat.uri.UriInject;
import net.kuujo.copycat.uri.UriPort;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultVertx;

/**
 * TCP based protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocol implements Protocol {
  private final Vertx vertx;
  private String host;
  private int port;

  public TcpProtocol() {
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public TcpProtocol(@UriArgument("vertx") Vertx vertx) {
    this.vertx = vertx;
  }

  @UriInject
  public TcpProtocol(@UriHost String host, @Optional @UriPort int port, @UriArgument("vertx") Vertx vertx) {
    this.host = host;
    this.port = port;
    this.vertx = vertx;
  }

  @UriInject
  public TcpProtocol(@UriHost String host, @Optional @UriPort int port) {
    this.host = host;
    this.port = port;
    this.vertx = new DefaultVertx();
  }

  @UriInject
  public TcpProtocol(@UriHost String host) {
    this(host, 0);
  }

  /**
   * Sets the protocol host.
   *
   * @param host The TCP host.
   * @return The TCP protocol.
   */
  public TcpProtocol setHost(String host) {
    this.host = host;
    return this;
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
   * Sets the protocol port.
   *
   * @param port The TCP port.
   * @return The TCP protocol.
   */
  public TcpProtocol setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Returns the protocol port.
   *
   * @return The TCP port.
   */
  public int getPort() {
    return port;
  }

  @Override
  public void init(CopyCatContext context) {
  }

  @Override
  public ProtocolServer createServer() {
    return new TcpProtocolServer(vertx, host, port);
  }

  @Override
  public ProtocolClient createClient() {
    return new TcpProtocolClient(vertx, host, port);
  }

}
