package net.kuujo.copycat.cluster;

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolUri;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Member {

  String address();

  ProtocolUri uri();

  Protocol protocol();

}
