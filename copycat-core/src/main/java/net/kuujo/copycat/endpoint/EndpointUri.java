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
package net.kuujo.copycat.endpoint;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.util.ServiceInfo;
import net.kuujo.copycat.util.ServiceLoader;

/**
 * An endpoint URI.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EndpointUri {
  private final String address;
  private final URI uri;
  private final ServiceInfo info;
  private final CopyCatContext context;

  public EndpointUri(String address, CopyCatContext context) {
    this.address = address;
    try {
      this.uri = new URI(address);
    } catch (URISyntaxException e) {
      throw new EndpointException(e);
    }
    this.info = ServiceLoader.load(String.format("net.kuujo.copycat.endpoint.%s", this.uri.getScheme()));
    this.context = context;
  }

  /**
   * Returns a boolean indicating whether a URI is valid.
   *
   * @param uri The endpoint URI.
   */
  public static boolean isValidUri(String uri) {
    URI ruri;
    try {
      ruri = new URI(uri);
    } catch (URISyntaxException e) {
      return false;
    }
    ServiceLoader.load(String.format("net.kuujo.copycat.endpoint.%s", ruri.getScheme()));
    return true;
  }

  /**
   * Returns the endpoint service name.
   *
   * @return The endpoint service name.
   */
  public String getServiceName() {
    return uri.getScheme();
  }

  /**
   * Returns the endpoint service info.
   *
   * @return The endpoint service info.
   */
  public ServiceInfo getServiceInfo() {
    return info;
  }

  /**
   * Returns the endpoint implementation class.
   *
   * @return The endpoint implementation class.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Endpoint> getEndpointClass() {
    return info.getProperty("class", Class.class);
  }

  /**
   * Returns a map of endpoint arguments.
   *
   * @return A map of endpoint query arguments.
   */
  public Map<String, Object> getEndpointArgs() {
    Map<String, Object> args = new LinkedHashMap<String, Object>();
    args.put("address", address);
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
