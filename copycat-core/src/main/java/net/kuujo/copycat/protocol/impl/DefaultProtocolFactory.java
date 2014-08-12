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
package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolFactory;
import net.kuujo.copycat.protocol.ProtocolUri;
import net.kuujo.copycat.uri.UriInjector;

/**
 * Protocol factory that injects URI arguments into the protocol instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultProtocolFactory implements ProtocolFactory {
  private final CopyCatContext context;

  public DefaultProtocolFactory(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public Protocol createProtocol(String uri) {
    ProtocolUri wrappedUri = new ProtocolUri(uri);
    Class<? extends Protocol> protocolClass = wrappedUri.getProtocolClass();
    UriInjector injector = new UriInjector(wrappedUri.getRawUri(), context.registry());
    Protocol protocol = injector.inject(protocolClass);
    protocol.init(context);
    return protocol;
  }

}
