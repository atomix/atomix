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
package net.kuujo.copycat.endpoint.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.EndpointUri;
import net.kuujo.copycat.uri.UriInjector;

/**
 * Endpoint factory that injects URI arguments into the endpoint instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEndpointFactory implements EndpointFactory {
  private final CopyCatContext context;

  public DefaultEndpointFactory(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public Endpoint createEndpoint(String uri) {
    EndpointUri wrappedUri = new EndpointUri(uri);
    Class<? extends Endpoint> endpointClass = wrappedUri.getEndpointClass();
    UriInjector injector = new UriInjector(wrappedUri.getRawUri(), context.registry());
    Endpoint endpoint = injector.inject(endpointClass);
    endpoint.init(context);
    return endpoint;
  }

}
