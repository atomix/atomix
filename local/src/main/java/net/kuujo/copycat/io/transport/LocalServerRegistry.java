/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.io.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local member registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalServerRegistry {
  private final Map<Address, LocalServer> registry = new ConcurrentHashMap<>();

  /**
   * Registers a server.
   *
   * @param address The server address.
   * @param server The server.
   */
  void register(Address address, LocalServer server) {
    registry.put(address, server);
  }

  /**
   * Unregisters a server.
   *
   * @param address The server address.
   */
  void unregister(Address address) {
    registry.remove(address);
  }

  /**
   * Looks up a server by address.
   *
   * @param address The server address.
   * @return The server or {@code null} if no server is registered at that address.
   */
  LocalServer get(Address address) {
    return registry.get(address);
  }

}
