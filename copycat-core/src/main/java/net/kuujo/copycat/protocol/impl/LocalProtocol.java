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

import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.uri.UriAuthority;
import net.kuujo.copycat.uri.UriSchemeSpecificPart;

/**
 * Local protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocol implements Protocol {
  private Registry registry;
  private String address;

  public LocalProtocol() {
  }

  @Override
  public String name() {
    return address;
  }

  /**
   * Sets the protocol registry.
   *
   * @param registry The protocol registry.
   */
  public void setRegistry(Registry registry) {
    this.registry = registry;
  }

  /**
   * Returns the protocol registry.
   *
   * @return The protocol registry.
   */
  public Registry getRegistry() {
    return registry;
  }

  /**
   * Sets the protocol address.
   *
   * @param address The protocol address.
   */
  @UriAuthority
  @UriSchemeSpecificPart
  public void setAddress(String address) {
    this.address = address;
  }

  /**
   * Returns the protocol address.
   *
   * @return The protocol address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the protocol address, returning the protocol for method chaining.
   *
   * @param address The protocol address.
   * @return The protocol instance.
   */
  public LocalProtocol withAddress(String address) {
    this.address = address;
    return this;
  }

  @Override
  public ProtocolServer createServer() {
    return new LocalProtocolServer(address, registry);
  }

  @Override
  public ProtocolClient createClient() {
    return new LocalProtocolClient(address, registry);
  }

}
