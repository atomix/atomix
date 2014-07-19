package net.kuujo.copycat.protocol;

import java.net.URI;
import java.net.URISyntaxException;

import net.kuujo.copycat.util.ServiceInfo;
import net.kuujo.copycat.util.ServiceLoader;

public class ProtocolUri {
  private final URI uri;
  private final ServiceInfo info;

  public ProtocolUri(String uri) {
    try {
      this.uri = new URI(uri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    info = ServiceLoader.load(String.format("net.kuujo.copycat.protocol.%s", this.uri.getScheme()));
  }

  public ProtocolUri(URI uri) {
    this.uri = uri;
    info = ServiceLoader.load(String.format("net.kuujo.copycat.protocol.%s", this.uri.getScheme()));
  }

  /**
   * Returns a boolean indicating whether a URI is valid.
   *
   * @param uri The protocol URI.
   */
  public static boolean isValidUri(String uri) {
    URI ruri;
    try {
      ruri = new URI(uri);
    } catch (URISyntaxException e) {
      return false;
    }
    ServiceLoader.load(String.format("net.kuujo.copycat.protocol.%s", ruri.getScheme()));
    return true;
  }

  /**
   * Returns the protocol service name.
   *
   * @return The protocol service name.
   */
  public String getServiceName() {
    return uri.getScheme();
  }

  /**
   * Returns the protocol service info.
   *
   * @return The protocol service info.
   */
  public ServiceInfo getServiceInfo() {
    return info;
  }

}
