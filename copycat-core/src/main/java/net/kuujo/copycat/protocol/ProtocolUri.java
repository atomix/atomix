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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.EndpointException;
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
  private final CopyCatContext context;

  public ProtocolUri(String uri, CopyCatContext context) {
    try {
      this.uri = new URI(uri);
    } catch (URISyntaxException e) {
      throw new ProtocolException(e);
    }
    info = ServiceLoader.load(String.format("net.kuujo.copycat.protocol.%s", this.uri.getScheme()));
    this.context = context;
  }

  public ProtocolUri(URI uri, CopyCatContext context) {
    this.uri = uri;
    info = ServiceLoader.load(String.format("net.kuujo.copycat.protocol.%s", this.uri.getScheme()));
    this.context = context;
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
   * Returns the protocol class.
   *
   * @return The protocol class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Protocol> getProtocolClass() {
    return info.getProperty("class", Class.class);
  }

  /**
   * Returns a map of protocol arguments.
   *
   * @return A map of protocol query arguments.
   */
  public Map<String, Object> getProtocolArgs() {
    Map<String, Object> args = new LinkedHashMap<String, Object>();
    args.put("context", context);
    for (String key : context.registry().keys()) {
      args.put(key, context.registry().lookup(key));
    }
    String query = uri.getQuery();
    if (query == null) {
      return args;
    }
    String[] pairs = query.split("&");
    for (String pair : pairs) {
      int index = pair.indexOf("=");
      try {
        args.put(URLDecoder.decode(pair.substring(0, index), "UTF-8"), URLDecoder.decode(pair.substring(index + 1), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new EndpointException(e);
      }
    }
    return args;
  }

}
