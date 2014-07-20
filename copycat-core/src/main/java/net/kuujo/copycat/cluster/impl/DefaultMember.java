package net.kuujo.copycat.cluster.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.ProtocolUri;

/**
 * Default cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultMember implements Member {
  private final String address;
  private final ProtocolUri uri;
  private final Protocol protocol;

  public DefaultMember(String address, CopyCatContext context) {
    this.address = address;
    this.uri = new ProtocolUri(address);
    Class<? extends Protocol> protocolClass = uri.getProtocolClass();
    try {
      protocol = protocolClass.newInstance();
      protocol.init(address, context);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ProtocolException(e);
    }
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public ProtocolUri uri() {
    return uri;
  }

  @Override
  public Protocol protocol() {
    return protocol;
  }

}
