package net.kuujo.copycat.protocol;

import net.kuujo.copycat.CopyCatContext;

/**
 * Base protocol provider.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {

  /**
   * Initializes the protocol.
   *
   * @param address The protocol address.
   * @param context The protocol context.
   */
  void init(String address, CopyCatContext context);

  /**
   * Returns a protocol server.
   *
   * @return The protocol server.
   */
  ProtocolServer server();

  /**
   * Returns a protocol client.
   *
   * @return The protocol client.
   */
  ProtocolClient client();

}
