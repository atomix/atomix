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
import java.net.URISyntaxException;

import net.kuujo.copycat.util.ServiceInfo;
import net.kuujo.copycat.util.ServiceLoader;

/**
 * A protocol URI.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
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

  /**
   * Returns the protocol server class.
   *
   * @return The protocol server class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends ProtocolServer> getServerClass() {
    return info.getProperty("server", Class.class);
  }

  /**
   * Returns the protocol client class.
   *
   * @return The protocol client class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends ProtocolClient> getClientClass() {
    return info.getProperty("client", Class.class);
  }

}
